"""Клиент Google Sheets — чтение каталога/текстов, запись лидов, аналитика,
управление статьями, дата-рум, новости, AI-диалоги, контент-календарь."""

import asyncio
import functools
import json as _json
import logging
from datetime import datetime, timezone
from typing import Optional

import gspread
from google.oauth2.service_account import Credentials

logger = logging.getLogger(__name__)


# ── Circuit Breaker / Retry ─────────────────────────────────────────────

_consecutive_failures: int = 0
_CIRCUIT_OPEN_THRESHOLD = 5


def retry_sheets(max_retries: int = 3, initial_delay: float = 1.0):
    """Декоратор: retry с экспоненциальным backoff для Google Sheets API.

    Автоматически трекает success/failure в мониторинг.
    """

    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            global _consecutive_failures
            delay = initial_delay
            last_exc = None
            method_name = func.__name__
            for attempt in range(max_retries):
                try:
                    result = func(*args, **kwargs)
                    _consecutive_failures = 0
                    _track_sheets("success", method_name)
                    return result
                except gspread.exceptions.APIError as e:
                    last_exc = e
                    status = getattr(getattr(e, "response", None), "status_code", 0)
                    if status == 429:
                        logger.warning(
                            "Sheets quota exceeded, retry %d/%d in %.1fs",
                            attempt + 1, max_retries, delay,
                        )
                        import time
                        time.sleep(delay)
                        delay *= 2
                    else:
                        _consecutive_failures += 1
                        _track_sheets("error", method_name)
                        raise
                except Exception as e:
                    last_exc = e
                    _consecutive_failures += 1
                    if attempt == max_retries - 1:
                        _track_sheets("error", method_name)
                        raise
                    logger.warning(
                        "Sheets attempt %d/%d failed: %s", attempt + 1, max_retries, e,
                    )
                    import time
                    time.sleep(delay)
                    delay *= 2
            _track_sheets("error", method_name)
            raise last_exc  # type: ignore[misc]

        return wrapper

    return decorator


def _track_sheets(result: str, method: str) -> None:
    """Трекает Sheets API вызов в мониторинг (best-effort)."""
    try:
        from src.bot.utils.monitoring import alerts, metrics
        if result == "success":
            metrics.inc("sheets.success")
        else:
            metrics.inc_error("sheets_api")
            import asyncio
            try:
                loop = asyncio.get_running_loop()
                loop.create_task(alerts.check_sheets_health(False, method))
            except RuntimeError:
                pass
    except Exception:
        pass


async def save_pending_write(method_name: str, payload: dict) -> None:
    """Сохраняет неудавшуюся запись в SQLite для повторной отправки."""
    try:
        from src.database.models import PendingSheetsWrite, async_session
        async with async_session() as session:
            pending = PendingSheetsWrite(
                method_name=method_name,
                payload_json=_json.dumps(payload, ensure_ascii=False, default=str),
            )
            session.add(pending)
            await session.commit()
            logger.info("Pending write saved: %s", method_name)
    except Exception as e:
        logger.error("Failed to save pending write: %s", e)

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

# Названия листов в Google Sheets
SHEET_CATALOG = "Каталог гайдов"
SHEET_TEXTS = "Тексты бота"
SHEET_LEADS = "Лиды"
SHEET_FOLLOWUP = "Авто-серия"
SHEET_ANALYTICS = "Аналитика"
SHEET_ARTICLES = "Статьи сайта"
SHEET_DATA_ROOM = "Data Room"
SHEET_NEWS = "News Feed"
SHEET_CONTENT_CAL = "Content Calendar"
SHEET_AI_CONV = "AI Conversations"
SHEET_CONSULT_LOG = "Consult Log"
SHEET_CONSULTATIONS = "Консультации"
SHEET_RECOMMENDATIONS = "Рекомендации"
SHEET_QUESTIONS = "Вопросы"


# ── Ожидаемые схемы критических листов ──────────────────────────────────
# Формат: sheet_name → {required: обязательные колонки, optional: желательные}
# Имена колонок сравниваются case-insensitive.

SHEET_SCHEMAS: dict[str, dict[str, list[str]]] = {
    SHEET_CATALOG: {
        "required": ["id", "title", "description", "drive_file_id", "active"],
        "optional": ["category", "preview_text", "social_proof", "pages", "highlights"],
    },
    SHEET_TEXTS: {
        "required": ["key", "text"],
        "optional": [],
    },
    SHEET_LEADS: {
        # Бот пишет позиционно (append_row), но читает по имени в
        # _sync_update_lead_interests — если колонки переименованы,
        # interests/warmth не обновятся, но запись лида не сломается.
        "required": [],
        "optional": ["user_id", "email", "guide", "timestamp", "username",
                      "name", "consent", "source", "interests", "warmth"],
    },
    SHEET_FOLLOWUP: {
        "required": ["key", "text"],
        "optional": ["content_url", "content_type", "button_text", "delay_hours"],
    },
    SHEET_RECOMMENDATIONS: {
        "required": ["guide_id", "next_guide_id"],
        "optional": ["next_article_link", "message"],
    },
    SHEET_CONSULTATIONS: {
        "required": ["User ID", "Телефон"],
        "optional": ["Дата", "Username", "Имя", "Email", "Удобное время", "Статус"],
    },
}


def _safe_get_all_records(ws) -> list[dict]:
    """Безопасно читает все записи из листа, даже при дубликатах заголовков."""
    try:
        return ws.get_all_records()
    except Exception:
        # Если заголовки дублируются или пусты — читаем вручную
        vals = ws.get_all_values()
        if not vals:
            return []
        header = vals[0]
        # Убираем пустые + делаем уникальными
        clean_header = []
        seen: dict[str, int] = {}
        for h in header:
            h = h.strip()
            if not h:
                h = f"_col_{len(clean_header)}"
            if h in seen:
                seen[h] += 1
                h = f"{h}_{seen[h]}"
            else:
                seen[h] = 0
            clean_header.append(h)
        return [dict(zip(clean_header, row)) for row in vals[1:]]


class GoogleSheetsClient:
    """Клиент для работы с Google Sheets (каталог, тексты, лиды).

    Args:
        credentials_path: Путь к JSON-файлу сервисного аккаунта.
        spreadsheet_id: ID Google-таблицы из URL.
    """

    def __init__(
        self,
        credentials_path: str,
        spreadsheet_id: str,
        *,
        credentials_json: Optional[str] = None,
        credentials_base64: Optional[str] = None,
    ) -> None:
        import os
        import tempfile

        if credentials_json and not os.path.exists(credentials_path):
            info = _json.loads(credentials_json)
            with tempfile.NamedTemporaryFile(
                mode="w", suffix=".json", delete=False, dir="/tmp"
            ) as f:
                _json.dump(info, f)
                credentials_path = f.name
            logger.info("Google credentials written to temp file from JSON env var")
        elif credentials_base64 and not os.path.exists(credentials_path):
            import base64 as _b64
            raw = _b64.b64decode(
                credentials_base64 + "=" * (-len(credentials_base64) % 4)
            )
            with tempfile.NamedTemporaryFile(
                mode="wb", suffix=".json", delete=False, dir="/tmp"
            ) as f:
                f.write(raw)
                credentials_path = f.name
            logger.info("Google credentials written to temp file from base64 env var")

        creds = Credentials.from_service_account_file(
            credentials_path,
            scopes=SCOPES,
        )
        self._creds = creds
        self.gc = gspread.authorize(creds)
        self.spreadsheet_id = spreadsheet_id
        self._spreadsheet: gspread.Spreadsheet | None = None

        # Кеш структуры листов: sheet_name → list[str] (заголовки)
        self._cached_headers: dict[str, list[str]] = {}
        # Результат последней валидации
        self.schema_warnings: list[str] = []
        self.schema_ok: bool = True

        logger.info("GoogleSheetsClient инициализирован (spreadsheet=%s)", spreadsheet_id)

    # ── Внутренние методы ───────────────────────────────────────────────

    def _get_spreadsheet(self) -> gspread.Spreadsheet:
        """Ленивое открытие таблицы (переподключение при ошибке)."""
        if self._spreadsheet is None:
            self._spreadsheet = self.gc.open_by_key(self.spreadsheet_id)
        return self._spreadsheet

    # ── Валидация структуры листов ──────────────────────────────────────

    def _sync_validate_sheets(self) -> tuple[bool, list[str]]:
        """Проверяет заголовки всех критических листов против SHEET_SCHEMAS.

        Returns:
            (all_ok, warnings) — True если все обязательные колонки на месте,
            список текстовых предупреждений для каждого несоответствия.
        """
        warnings: list[str] = []
        all_ok = True
        spreadsheet = self._get_spreadsheet()

        for sheet_name, schema in SHEET_SCHEMAS.items():
            try:
                ws = spreadsheet.worksheet(sheet_name)
            except gspread.exceptions.WorksheetNotFound:
                # Необязательные листы (Рекомендации, Консультации) могут
                # создаваться динамически — предупреждаем, но не фейлим.
                warnings.append(f"⚠️ Лист «{sheet_name}» не найден в таблице")
                logger.warning("Schema validation: лист '%s' отсутствует", sheet_name)
                continue
            except Exception as exc:
                warnings.append(f"❌ Ошибка чтения листа «{sheet_name}»: {exc}")
                logger.error("Schema validation error for '%s': %s", sheet_name, exc)
                all_ok = False
                continue

            try:
                header_raw = ws.row_values(1)
            except Exception as exc:
                warnings.append(f"❌ Не удалось прочитать заголовки «{sheet_name}»: {exc}")
                all_ok = False
                continue

            header_lower = [h.strip().lower() for h in header_raw]

            # Кешируем реальные заголовки
            self._cached_headers[sheet_name] = header_raw

            # Проверяем обязательные колонки
            required = schema.get("required", [])
            missing_req = [
                col for col in required
                if col.lower() not in header_lower
            ]
            if missing_req:
                all_ok = False
                msg = (
                    f"❌ «{sheet_name}»: отсутствуют обязательные колонки: "
                    f"{', '.join(missing_req)}"
                )
                warnings.append(msg)
                logger.error("Schema drift: %s", msg)

            # Проверяем опциональные (только предупреждение)
            optional = schema.get("optional", [])
            missing_opt = [
                col for col in optional
                if col.lower() not in header_lower
            ]
            if missing_opt:
                msg = (
                    f"⚠️ «{sheet_name}»: нет опциональных колонок: "
                    f"{', '.join(missing_opt)}"
                )
                warnings.append(msg)
                logger.warning("Schema note: %s", msg)

            # Обнаруживаем неизвестные колонки (инфо)
            known = {c.lower() for c in required + optional}
            extra = [h for h in header_raw if h.strip() and h.strip().lower() not in known]
            if extra:
                logger.info(
                    "Schema info: '%s' содержит дополнительные колонки: %s",
                    sheet_name, ", ".join(extra),
                )

        return all_ok, warnings

    async def validate_sheets(self) -> tuple[bool, list[str]]:
        """Асинхронная валидация структуры листов.

        Кеширует заголовки и возвращает (ok, warnings).
        Используется при старте бота и по /health.
        """
        ok, warnings = await asyncio.to_thread(self._sync_validate_sheets)
        self.schema_ok = ok
        self.schema_warnings = warnings
        return ok, warnings

    def get_cached_headers(self, sheet_name: str) -> list[str] | None:
        """Возвращает закешированные заголовки листа (после validate_sheets)."""
        return self._cached_headers.get(sheet_name)

    # ── Каталог гайдов ──────────────────────────────────────────────────

    @retry_sheets()
    def _sync_get_catalog(self) -> list[dict]:
        """Синхронное чтение листа «Каталог гайдов»."""
        try:
            ws = self._get_spreadsheet().worksheet(SHEET_CATALOG)
            rows = _safe_get_all_records(ws)
            # Фильтруем только активные гайды
            active = [
                row for row in rows
                if str(row.get("active", "")).upper() == "TRUE"
            ]
            logger.info("Загружено %d активных гайдов из Sheets", len(active))
            return active
        except gspread.exceptions.WorksheetNotFound:
            logger.error("Лист '%s' не найден в таблице", SHEET_CATALOG)
            return []
        except Exception as e:
            logger.error("Ошибка чтения каталога: %s", e)
            raise

    async def get_guides_catalog(self) -> list[dict]:
        """Асинхронное чтение каталога гайдов.

        Returns:
            Список словарей с полями: id, title, description,
            drive_file_id, category, active.
        """
        return await asyncio.to_thread(self._sync_get_catalog)

    # ── Тексты бота ─────────────────────────────────────────────────────

    @retry_sheets()
    def _sync_get_texts(self) -> dict[str, str]:
        """Синхронное чтение листа «Тексты бота»."""
        try:
            ws = self._get_spreadsheet().worksheet(SHEET_TEXTS)
            rows = _safe_get_all_records(ws)
            texts = {
                str(row.get("key", "")): str(row.get("text", ""))
                for row in rows
                if row.get("key")
            }
            logger.info("Загружено %d текстов из Sheets", len(texts))
            return texts
        except gspread.exceptions.WorksheetNotFound:
            logger.error("Лист '%s' не найден в таблице", SHEET_TEXTS)
            return {}
        except Exception as e:
            logger.error("Ошибка чтения текстов: %s", e)
            raise

    async def get_bot_texts(self) -> dict[str, str]:
        """Асинхронное чтение текстов бота.

        Returns:
            Словарь ``{key: text}``, например
            ``{"welcome_not_subscribed": "Для получения..."}``.
        """
        return await asyncio.to_thread(self._sync_get_texts)

    # ── Запись лидов ────────────────────────────────────────────────────

    @retry_sheets()
    def _sync_append_lead(self, lead_row: list) -> None:
        """Синхронная запись строки в лист «Лиды»."""
        try:
            ws = self._get_spreadsheet().worksheet(SHEET_LEADS)
            ws.append_row(lead_row, value_input_option="USER_ENTERED")
            logger.info("Лид записан в Google Sheets")
        except gspread.exceptions.WorksheetNotFound:
            logger.error("Лист '%s' не найден в таблице", SHEET_LEADS)
        except Exception as e:
            logger.error("Ошибка записи лида в Sheets: %s", e)

    @retry_sheets()
    def _sync_update_lead_interests(self, user_id: int, guide: str) -> None:
        """Обновляет колонки 'Интересы' и 'Warmth' для существующего лида.

        Ищет строки с данным user_id и дополняет их:
        - Интересы: список скачанных гайдов через запятую.
        - Warmth: Cold -> Warm (после скачивания гайда), Hot (после 3+ гайдов).
        """
        try:
            ws = self._get_spreadsheet().worksheet(SHEET_LEADS)
            all_rows = _safe_get_all_records(ws)

            # Собираем все гайды, скачанные этим пользователем
            user_guides = set()
            user_row_indices = []  # 1-indexed (для gspread)
            for idx, row in enumerate(all_rows, start=2):  # +2: header + 0-indexed
                if str(row.get("user_id", "")) == str(user_id):
                    user_row_indices.append(idx)
                    g = str(row.get("guide", "")).strip()
                    if g:
                        user_guides.add(g)

            user_guides.add(guide)  # Добавляем текущий

            # Определяем уровень warmth
            guide_count = len(user_guides)
            if guide_count >= 3:
                warmth = "Hot"
            elif guide_count >= 1:
                warmth = "Warm"
            else:
                warmth = "Cold"

            interests = ", ".join(sorted(user_guides))

            # Проверяем, есть ли колонки «interests» и «warmth» в заголовке
            header = ws.row_values(1)
            interests_col = None
            warmth_col = None
            for i, h in enumerate(header, start=1):
                if h.lower() == "interests":
                    interests_col = i
                elif h.lower() == "warmth":
                    warmth_col = i

            # Если колонок нет — добавляем
            if interests_col is None:
                interests_col = len(header) + 1
                ws.update_cell(1, interests_col, "interests")
            if warmth_col is None:
                warmth_col = len(header) + 2 if interests_col == len(header) + 1 else len(header) + 1
                ws.update_cell(1, warmth_col, "warmth")

            # Обновляем все строки пользователя
            for row_idx in user_row_indices:
                ws.update_cell(row_idx, interests_col, interests)
                ws.update_cell(row_idx, warmth_col, warmth)

            logger.info(
                "CRM обновлен: user_id=%s, interests=%s, warmth=%s",
                user_id, interests, warmth,
            )
        except Exception as e:
            logger.error("Ошибка обновления interests/warmth: %s", e)

    async def append_lead(
        self,
        *,
        user_id: int,
        username: str,
        name: str,
        email: str,
        guide: str,
        consent: bool = True,
        source: str = "",
    ) -> None:
        """Добавляет строку лида в лист «Лиды».

        Args:
            user_id: Telegram ID пользователя.
            username: Username в Telegram.
            name: Имя пользователя.
            email: Email пользователя.
            guide: ID выбранного гайда.
            consent: Дано ли согласие.
            source: Источник трафика (deep-link).
        """
        now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        row = [
            now,
            str(user_id),
            username or "",
            name,
            email,
            guide,
            "Да" if consent else "Нет",
            source,
        ]
        await asyncio.to_thread(self._sync_append_lead, row)

        # Обновляем interests/warmth для CRM-интеграции между ботами
        asyncio.create_task(
            asyncio.to_thread(self._sync_update_lead_interests, user_id, guide)
        )

        # Обновляем аналитику после каждого лида
        asyncio.create_task(self.update_analytics())

    # ── Обновление сферы бизнеса ──────────────────────────────────────────

    @retry_sheets()
    def _sync_update_lead_sphere(self, user_id: int, sphere: str) -> None:
        """Обновляет колонку 'business_sphere' для лида в CRM.

        Если колонки ещё нет — создаёт.
        """
        try:
            ws = self._get_spreadsheet().worksheet(SHEET_LEADS)
            all_rows = _safe_get_all_records(ws)
            header = ws.row_values(1)

            # Ищем (или создаём) колонку sphere
            sphere_col = None
            for i, h in enumerate(header, start=1):
                if h.lower() in ("business_sphere", "сфера", "sphere"):
                    sphere_col = i
                    break
            if sphere_col is None:
                sphere_col = len(header) + 1
                ws.update_cell(1, sphere_col, "business_sphere")

            # Обновляем все строки пользователя
            updated = 0
            for idx, row in enumerate(all_rows, start=2):
                if str(row.get("user_id", "")) == str(user_id):
                    ws.update_cell(idx, sphere_col, sphere)
                    updated += 1

            if updated:
                logger.info(
                    "CRM sphere updated: user_id=%s, sphere='%s', rows=%d",
                    user_id, sphere[:50], updated,
                )
        except Exception as e:
            logger.error("Ошибка обновления sphere в CRM: %s", e)

    async def update_lead_sphere(self, user_id: int, sphere: str) -> None:
        """Асинхронно обновляет сферу бизнеса лида в Google Sheets."""
        await asyncio.to_thread(self._sync_update_lead_sphere, user_id, sphere)

    # ── Авто-серия follow-up ────────────────────────────────────────────

    @retry_sheets()
    def _sync_get_followup_series(self) -> list[dict]:
        """Синхронное чтение листа «Авто-серия» как list[dict].

        Каждая строка содержит: key, text, content_url, content_type, button_text.
        """
        try:
            ws = self._get_spreadsheet().worksheet(SHEET_FOLLOWUP)
            rows = _safe_get_all_records(ws)
            result = [row for row in rows if row.get("key")]
            logger.info("Загружено %d записей авто-серии из Sheets", len(result))
            return result
        except gspread.exceptions.WorksheetNotFound:
            logger.warning("Лист '%s' не найден — используются fallback-тексты", SHEET_FOLLOWUP)
            return []
        except Exception as e:
            logger.error("Ошибка чтения авто-серии: %s", e)
            return []

    async def get_followup_series(self) -> list[dict]:
        """Асинхронное чтение записей авто-серии.

        Returns:
            Список словарей. Каждая строка может содержать:
            - ``key``: ключ (``step_0``, ``step_1``, ``step_2``,
              или ``{guide_id}_step_{N}`` для гайд-специфичных,
              или ``{sphere}_step_{N}`` для сфер-специфичных).
            - ``text``: основной текст сообщения (HTML).
            - ``content_url``: ссылка на контент (статья, чек-лист).
            - ``content_type``: тип контента (checklist, article, webinar).
            - ``button_text``: текст кнопки для ``content_url``.
        """
        return await asyncio.to_thread(self._sync_get_followup_series)

    # ── Аналитика ───────────────────────────────────────────────────────

    @retry_sheets()
    def _sync_update_analytics(self) -> None:
        """Синхронное обновление листа «Аналитика» на основе данных из «Лиды»."""
        try:
            spreadsheet = self._get_spreadsheet()

            # Получаем лист «Аналитика» или создаём
            try:
                ws_analytics = spreadsheet.worksheet(SHEET_ANALYTICS)
            except gspread.exceptions.WorksheetNotFound:
                ws_analytics = spreadsheet.add_worksheet(
                    title=SHEET_ANALYTICS, rows=50, cols=5
                )

            # Читаем лиды
            try:
                ws_leads = spreadsheet.worksheet(SHEET_LEADS)
                leads = _safe_get_all_records(ws_leads)
            except gspread.exceptions.WorksheetNotFound:
                leads = []

            # Читаем каталог для подсчёта
            try:
                ws_catalog = spreadsheet.worksheet(SHEET_CATALOG)
                catalog = _safe_get_all_records(ws_catalog)
            except gspread.exceptions.WorksheetNotFound:
                catalog = []

            # Считаем метрики
            total_leads = len(leads)
            unique_users = len({row.get("user_id", "") for row in leads if row.get("user_id")})
            total_guides = len(catalog)

            # Топ гайдов
            guide_counts: dict[str, int] = {}
            for row in leads:
                g = str(row.get("guide", ""))
                if g:
                    guide_counts[g] = guide_counts.get(g, 0) + 1
            top_guides = sorted(guide_counts.items(), key=lambda x: x[1], reverse=True)[:5]

            # Источники трафика
            source_counts: dict[str, int] = {}
            for row in leads:
                s = str(row.get("source", "")).strip()
                if s:
                    source_counts[s] = source_counts.get(s, 0) + 1
            top_sources = sorted(source_counts.items(), key=lambda x: x[1], reverse=True)[:5]

            # Лиды по дням
            daily_counts: dict[str, int] = {}
            for row in leads:
                date_str = str(row.get("timestamp", ""))[:10]  # YYYY-MM-DD
                if date_str:
                    daily_counts[date_str] = daily_counts.get(date_str, 0) + 1

            # Формируем данные для листа
            now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
            data = [
                ["📊 Аналитика бота SOLIS Partners", "", ""],
                [f"Обновлено: {now}", "", ""],
                ["", "", ""],
                ["Метрика", "Значение", ""],
                ["Всего лидов", str(total_leads), ""],
                ["Уникальных пользователей", str(unique_users), ""],
                ["Гайдов в каталоге", str(total_guides), ""],
                [
                    "Конверсия (лиды/пользователи)",
                    f"{(total_leads / unique_users * 100):.1f}%" if unique_users else "0%",
                    "",
                ],
                ["", "", ""],
                ["📚 Топ скачиваемых гайдов", "Скачиваний", ""],
            ]
            for guide_name, count in top_guides:
                data.append([guide_name, str(count), ""])

            data.append(["", "", ""])
            data.append(["📍 Источники трафика", "Лидов", ""])
            if top_sources:
                for src_name, count in top_sources:
                    data.append([src_name, str(count), ""])
            else:
                data.append(["(нет данных по источникам)", "", ""])

            data.append(["", "", ""])
            data.append(["📅 Лиды по дням", "Количество", ""])
            for date_key in sorted(daily_counts.keys(), reverse=True)[:14]:
                data.append([date_key, str(daily_counts[date_key]), ""])

            # Очищаем лист и записываем
            ws_analytics.clear()
            ws_analytics.update(values=data, range_name="A1")

            logger.info("Аналитика обновлена: %d лидов, %d пользователей", total_leads, unique_users)

        except Exception as e:
            logger.error("Ошибка обновления аналитики: %s", e)

    async def update_analytics(self) -> None:
        """Асинхронное обновление листа «Аналитика»."""
        await asyncio.to_thread(self._sync_update_analytics)

    # ── Статьи сайта ────────────────────────────────────────────────────

    @retry_sheets()
    def _sync_append_article(self, row: list) -> None:
        """Синхронная запись статьи в лист «Статьи сайта»."""
        try:
            ws = self._get_spreadsheet().worksheet("Статьи сайта")
            ws.append_row(row, value_input_option="USER_ENTERED")
            logger.info("Статья записана в Google Sheets")
        except gspread.exceptions.WorksheetNotFound:
            logger.error("Лист 'Статьи сайта' не найден")
        except Exception as e:
            logger.error("Ошибка записи статьи: %s", e)

    async def append_article(
        self,
        *,
        article_id: str,
        title: str,
        date: str,
        author: str,
        category: str,
        category_ru: str,
        description: str,
        external_url: str = "",
        content: str = "",
        is_gold: bool = False,
        practice_ids: str = "",
        telegram_bot_link: str = "",
        telegram_bot_cta_title: str = "",
        telegram_bot_cta_desc: str = "",
    ) -> None:
        """Добавляет статью в лист «Статьи сайта»."""
        row = [
            article_id,
            title,
            date,
            author,
            category,
            category_ru,
            "/assets/logo-solis.jpg",
            description,
            external_url,
            content,
            "TRUE" if is_gold else "",
            practice_ids,
            telegram_bot_link,
            telegram_bot_cta_title,
            telegram_bot_cta_desc,
            "TRUE",
        ]
        await asyncio.to_thread(self._sync_append_article, row)

    # ── Каталог гайдов (добавление) ─────────────────────────────────────

    @retry_sheets()
    def _sync_append_guide(self, row: list) -> None:
        """Синхронная запись гайда в лист «Каталог гайдов»."""
        try:
            ws = self._get_spreadsheet().worksheet(SHEET_CATALOG)
            ws.append_row(row, value_input_option="USER_ENTERED")
            logger.info("Гайд добавлен в каталог")
        except Exception as e:
            logger.error("Ошибка записи гайда: %s", e)

    async def append_guide(
        self,
        *,
        guide_id: str,
        title: str,
        description: str,
        drive_file_id: str,
        category: str = "",
    ) -> None:
        """Добавляет гайд в каталог."""
        row = [guide_id, title, description, drive_file_id, category, "TRUE"]
        await asyncio.to_thread(self._sync_append_guide, row)

    # ── Google Drive (загрузка файлов) ──────────────────────────────────

    @retry_sheets()
    def _sync_upload_to_drive(
        self, local_path: str, filename: str, folder_id: str
    ) -> str | None:
        """Загружает файл в папку Google Drive через API."""
        import json
        import urllib.error
        import urllib.request
        from io import BytesIO

        try:
            # Получаем access token из сохранённых credentials
            from google.auth.transport.requests import Request as AuthRequest

            creds = self._creds
            # Всегда рефрешим токен для актуальных скоупов
            creds.refresh(AuthRequest())
            token = creds.token

            logger.info(
                "Drive upload: token получен, scopes=%s, email=%s",
                getattr(creds, "scopes", "N/A"),
                getattr(creds, "service_account_email", "N/A"),
            )

            # Сначала пробуем загрузить без указания папки (в корень Drive сервисного аккаунта)
            # потом переместим если folder_id указан
            metadata = {"name": filename}
            if folder_id:
                metadata["parents"] = [folder_id]

            boundary = "----UploadBoundary7MA4YWxkTrZu0gW"

            body = BytesIO()
            # Part 1: metadata
            body.write(f"--{boundary}\r\n".encode())
            body.write(b"Content-Type: application/json; charset=UTF-8\r\n\r\n")
            body.write(json.dumps(metadata).encode("utf-8"))
            body.write(b"\r\n")

            # Part 2: file
            body.write(f"--{boundary}\r\n".encode())
            body.write(b"Content-Type: application/pdf\r\n\r\n")
            with open(local_path, "rb") as f:
                body.write(f.read())
            body.write(b"\r\n")
            body.write(f"--{boundary}--\r\n".encode())

            data = body.getvalue()

            req = urllib.request.Request(
                "https://www.googleapis.com/upload/drive/v3/files?uploadType=multipart&supportsAllDrives=true",
                data=data,
                method="POST",
            )
            req.add_header("Authorization", f"Bearer {token}")
            req.add_header(
                "Content-Type", f"multipart/related; boundary={boundary}"
            )

            try:
                with urllib.request.urlopen(req, timeout=120) as resp:
                    result = json.loads(resp.read().decode("utf-8"))
            except urllib.error.HTTPError as http_err:
                error_body = http_err.read().decode("utf-8", errors="replace")
                logger.error(
                    "Drive API HTTP %d: %s\nBody: %s",
                    http_err.code,
                    http_err.reason,
                    error_body[:500],
                )
                # Если 403 на папку — попробуем без папки
                if http_err.code == 403 and folder_id:
                    logger.info("Пробуем загрузить без указания папки...")
                    metadata_no_folder = {"name": filename}
                    body2 = BytesIO()
                    body2.write(f"--{boundary}\r\n".encode())
                    body2.write(b"Content-Type: application/json; charset=UTF-8\r\n\r\n")
                    body2.write(json.dumps(metadata_no_folder).encode("utf-8"))
                    body2.write(b"\r\n")
                    body2.write(f"--{boundary}\r\n".encode())
                    body2.write(b"Content-Type: application/pdf\r\n\r\n")
                    with open(local_path, "rb") as f:
                        body2.write(f.read())
                    body2.write(b"\r\n")
                    body2.write(f"--{boundary}--\r\n".encode())

                    req2 = urllib.request.Request(
                        "https://www.googleapis.com/upload/drive/v3/files?uploadType=multipart",
                        data=body2.getvalue(),
                        method="POST",
                    )
                    req2.add_header("Authorization", f"Bearer {token}")
                    req2.add_header(
                        "Content-Type", f"multipart/related; boundary={boundary}"
                    )
                    with urllib.request.urlopen(req2, timeout=120) as resp2:
                        result = json.loads(resp2.read().decode("utf-8"))
                else:
                    return None

            file_id = result.get("id", "")
            logger.info("Файл загружен в Drive: %s (id=%s)", filename, file_id)

            # Делаем файл доступным по ссылке
            try:
                permission_body = json.dumps(
                    {"role": "reader", "type": "anyone"}
                ).encode("utf-8")
                perm_req = urllib.request.Request(
                    f"https://www.googleapis.com/drive/v3/files/{file_id}/permissions",
                    data=permission_body,
                    method="POST",
                )
                perm_req.add_header("Authorization", f"Bearer {token}")
                perm_req.add_header("Content-Type", "application/json")
                urllib.request.urlopen(perm_req, timeout=15)
                logger.info("Файл расшарен: anyone -> reader")
            except Exception as e:
                logger.warning("Не удалось расшарить файл: %s", e)

            return file_id

        except Exception as e:
            logger.error("Ошибка загрузки в Drive: %s", e)
            return None

    async def upload_to_drive(
        self, local_path: str, filename: str, folder_id: str
    ) -> str | None:
        """Асинхронная загрузка файла в Google Drive.

        Returns:
            ID файла в Drive или None при ошибке.
        """
        return await asyncio.to_thread(
            self._sync_upload_to_drive, local_path, filename, folder_id
        )

    # ── Data Room (знания о компании) ────────────────────────────────────

    @retry_sheets()
    def _sync_get_data_room(self) -> list[dict]:
        """Читает лист «Data Room» — контекст компании для AI."""
        try:
            ws = self._get_spreadsheet().worksheet(SHEET_DATA_ROOM)
            return _safe_get_all_records(ws)
        except gspread.exceptions.WorksheetNotFound:
            logger.warning("Лист '%s' не найден", SHEET_DATA_ROOM)
            return []
        except Exception as e:
            logger.error("Ошибка чтения Data Room: %s", e)
            return []

    async def get_data_room(self) -> list[dict]:
        """Асинхронно получает данные Data Room."""
        return await asyncio.to_thread(self._sync_get_data_room)

    @retry_sheets()
    def _sync_append_data_room(self, row: list) -> None:
        try:
            ws = self._get_spreadsheet().worksheet(SHEET_DATA_ROOM)
            ws.append_row(row, value_input_option="USER_ENTERED")
        except Exception as e:
            logger.error("Ошибка записи в Data Room: %s", e)

    async def append_data_room(self, *, category: str, title: str, content: str) -> None:
        """Добавляет запись в дата-рум."""
        now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M")
        row = [category, title, content, now]
        await asyncio.to_thread(self._sync_append_data_room, row)

    # ── News Feed ────────────────────────────────────────────────────────

    @retry_sheets()
    def _sync_append_news(self, row: list) -> None:
        try:
            ws = self._get_spreadsheet().worksheet(SHEET_NEWS)
            ws.append_row(row, value_input_option="USER_ENTERED")
        except gspread.exceptions.WorksheetNotFound:
            logger.warning("Лист '%s' не найден", SHEET_NEWS)
        except Exception as e:
            logger.error("Ошибка записи новости: %s", e)

    async def append_news(
        self, *, source: str, title: str, url: str, summary: str = ""
    ) -> None:
        """Сохраняет новость в лист «News Feed»."""
        now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M")
        row = [now, source, title, url, summary, ""]
        await asyncio.to_thread(self._sync_append_news, row)

    @retry_sheets()
    def _sync_get_recent_news(self, limit: int = 20) -> list[dict]:
        try:
            ws = self._get_spreadsheet().worksheet(SHEET_NEWS)
            rows = _safe_get_all_records(ws)
            return rows[-limit:] if len(rows) > limit else rows
        except gspread.exceptions.WorksheetNotFound:
            return []
        except Exception as e:
            logger.error("Ошибка чтения новостей: %s", e)
            return []

    async def get_recent_news(self, limit: int = 20) -> list[dict]:
        return await asyncio.to_thread(self._sync_get_recent_news, limit)

    # ── Лиды (расширенные методы) ────────────────────────────────────────

    @retry_sheets()
    def _sync_get_recent_leads(self, limit: int = 50) -> list[dict]:
        try:
            ws = self._get_spreadsheet().worksheet(SHEET_LEADS)
            rows = _safe_get_all_records(ws)
            return rows[-limit:] if len(rows) > limit else rows
        except Exception as e:
            logger.error("Ошибка чтения лидов: %s", e)
            return []

    async def get_recent_leads(self, limit: int = 50) -> list[dict]:
        """Получает последние N лидов из Sheets."""
        return await asyncio.to_thread(self._sync_get_recent_leads, limit)

    # ── Статьи (список, toggle) ──────────────────────────────────────────

    @retry_sheets()
    def _sync_get_articles_list(self, limit: int = 20) -> list[dict]:
        try:
            ws = self._get_spreadsheet().worksheet(SHEET_ARTICLES)
            rows = _safe_get_all_records(ws)
            return rows[-limit:] if len(rows) > limit else rows
        except gspread.exceptions.WorksheetNotFound:
            return []
        except Exception as e:
            logger.error("Ошибка чтения статей: %s", e)
            return []

    async def get_articles_list(self, limit: int = 20) -> list[dict]:
        """Возвращает список статей."""
        return await asyncio.to_thread(self._sync_get_articles_list, limit)

    async def get_article_by_id(self, article_id: str) -> dict | None:
        """Ищет статью по id/slug среди всех записей."""
        articles = await self.get_articles_list(limit=200)
        slug_lower = article_id.lower().replace("-", " ")
        for art in articles:
            aid = str(art.get("id", art.get("article_id", ""))).lower()
            if aid == article_id.lower() or aid.replace("-", " ") == slug_lower:
                return art
        return None

    @retry_sheets()
    def _sync_toggle_article(self, article_id: str) -> bool:
        """Переключает active для статьи. Возвращает новое состояние."""
        try:
            ws = self._get_spreadsheet().worksheet(SHEET_ARTICLES)
            rows = _safe_get_all_records(ws)
            header = ws.row_values(1)

            id_col = None
            active_col = None
            for i, h in enumerate(header):
                if h.lower() in ("id", "article_id"):
                    id_col = i
                elif h.lower() == "active":
                    active_col = i

            if id_col is None or active_col is None:
                return False

            for idx, row_data in enumerate(rows, start=2):
                row_values = list(row_data.values())
                if str(row_values[id_col]) == article_id:
                    current = str(row_values[active_col]).upper()
                    new_val = "FALSE" if current == "TRUE" else "TRUE"
                    ws.update_cell(idx, active_col + 1, new_val)
                    return new_val == "TRUE"
            return False
        except Exception as e:
            logger.error("toggle_article error: %s", e)
            return False

    async def toggle_article(self, article_id: str) -> bool:
        return await asyncio.to_thread(self._sync_toggle_article, article_id)

    # ── Гайды (удаление) ─────────────────────────────────────────────────

    @retry_sheets()
    def _sync_delete_guide(self, guide_id: str) -> bool:
        try:
            ws = self._get_spreadsheet().worksheet(SHEET_CATALOG)
            rows = _safe_get_all_records(ws)
            for idx, row in enumerate(rows, start=2):
                if str(row.get("id", "")) == guide_id:
                    ws.delete_rows(idx)
                    logger.info("Гайд удалён из каталога: %s", guide_id)
                    return True
            return False
        except Exception as e:
            logger.error("delete_guide error: %s", e)
            return False

    async def delete_guide(self, guide_id: str) -> bool:
        return await asyncio.to_thread(self._sync_delete_guide, guide_id)

    # ── AI Conversations ─────────────────────────────────────────────────

    @retry_sheets()
    def _sync_log_ai_conversation(self, admin_msg: str, ai_resp: str) -> None:
        try:
            ws = self._get_spreadsheet().worksheet(SHEET_AI_CONV)
            now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M")
            ws.append_row(
                [now, admin_msg[:500], ai_resp[:500]],
                value_input_option="USER_ENTERED",
            )
        except gspread.exceptions.WorksheetNotFound:
            logger.warning("Лист '%s' не найден", SHEET_AI_CONV)
        except Exception as e:
            logger.error("Ошибка записи AI-диалога: %s", e)

    async def log_ai_conversation(self, *, admin_message: str, ai_response: str) -> None:
        await asyncio.to_thread(
            self._sync_log_ai_conversation, admin_message, ai_response
        )

    # ── Consult Log (логирование вопросов пользователей) ─────────────────

    @retry_sheets()
    def _sync_log_consult(self, user_id: int, question: str, answer: str) -> None:
        try:
            ws = self._get_spreadsheet().worksheet(SHEET_CONSULT_LOG)
            now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M")
            ws.append_row(
                [now, str(user_id), question[:300], answer[:300]],
                value_input_option="USER_ENTERED",
            )
        except gspread.exceptions.WorksheetNotFound:
            logger.warning("Лист '%s' не найден", SHEET_CONSULT_LOG)
        except Exception as e:
            logger.error("Ошибка записи consult: %s", e)

    async def log_consult(self, *, user_id: int, question: str, answer: str) -> None:
        """Логирует вопрос из /consult для Auto-FAQ."""
        await asyncio.to_thread(self._sync_log_consult, user_id, question, answer)

    @retry_sheets()
    def _sync_get_consult_log(self, limit: int = 100) -> list[dict]:
        try:
            ws = self._get_spreadsheet().worksheet(SHEET_CONSULT_LOG)
            rows = _safe_get_all_records(ws)
            return rows[-limit:] if len(rows) > limit else rows
        except gspread.exceptions.WorksheetNotFound:
            return []
        except Exception as e:
            logger.error("Ошибка чтения consult log: %s", e)
            return []

    async def get_consult_log(self, limit: int = 100) -> list[dict]:
        return await asyncio.to_thread(self._sync_get_consult_log, limit)

    # ── Консультации (заявки на звонок) ──────────────────────────────────

    @retry_sheets()
    def _sync_append_consultation(self, row: list) -> None:
        try:
            ws = self._get_spreadsheet().worksheet(SHEET_CONSULTATIONS)
            ws.append_row(row, value_input_option="USER_ENTERED")
        except gspread.exceptions.WorksheetNotFound:
            spreadsheet = self._get_spreadsheet()
            ws = spreadsheet.add_worksheet(title=SHEET_CONSULTATIONS, rows=100, cols=8)
            ws.append_row(
                ["Дата", "User ID", "Username", "Имя", "Email", "Телефон", "Удобное время", "Статус"],
                value_input_option="USER_ENTERED",
            )
            ws.append_row(row, value_input_option="USER_ENTERED")
            logger.info("Лист '%s' создан автоматически", SHEET_CONSULTATIONS)

    async def append_consultation(
        self,
        *,
        user_id: int,
        username: str,
        name: str,
        email: str,
        phone: str,
        preferred_time: str,
    ) -> None:
        """Сохраняет заявку на консультацию."""
        now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M")
        row = [now, str(user_id), username, name, email, phone, preferred_time, "Новая"]
        await asyncio.to_thread(self._sync_append_consultation, row)

    # ── Content Calendar ─────────────────────────────────────────────────

    @retry_sheets()
    def _sync_append_content_plan(self, row: list) -> None:
        try:
            ws = self._get_spreadsheet().worksheet(SHEET_CONTENT_CAL)
            ws.append_row(row, value_input_option="USER_ENTERED")
        except gspread.exceptions.WorksheetNotFound:
            logger.warning("Лист '%s' не найден", SHEET_CONTENT_CAL)
        except Exception as e:
            logger.error("Ошибка записи в контент-план: %s", e)

    async def append_content_plan(
        self, *, date: str, content_type: str, title: str, status: str = "planned"
    ) -> None:
        row = [date, content_type, title, status, ""]
        await asyncio.to_thread(self._sync_append_content_plan, row)

    @retry_sheets()
    def _sync_get_content_calendar(self) -> list[dict]:
        try:
            ws = self._get_spreadsheet().worksheet(SHEET_CONTENT_CAL)
            return _safe_get_all_records(ws)
        except gspread.exceptions.WorksheetNotFound:
            return []
        except Exception as e:
            logger.error("Ошибка чтения контент-плана: %s", e)
            return []

    async def get_content_calendar(self) -> list[dict]:
        return await asyncio.to_thread(self._sync_get_content_calendar)

    # ── Рекомендации (маппинг гайд → следующий гайд / статья) ──────────

    @retry_sheets()
    def _sync_get_recommendations(self) -> dict:
        """Читает лист 'Рекомендации': guide_id → {next_guide_id, next_article_link, message}."""
        try:
            ws = self._get_spreadsheet().worksheet(SHEET_RECOMMENDATIONS)
            rows = _safe_get_all_records(ws)
            mapping = {}
            for row in rows:
                gid = str(row.get("guide_id", row.get("id", ""))).strip()
                if gid:
                    mapping[gid] = {
                        "next_guide_id": str(row.get("next_guide_id", "")).strip(),
                        "next_article_link": str(row.get("next_article_link", "")).strip(),
                        "message": str(row.get("message", "")).strip(),
                    }
            return mapping
        except gspread.exceptions.WorksheetNotFound:
            logger.info("Лист '%s' не найден — рекомендации отключены", SHEET_RECOMMENDATIONS)
            return {}
        except Exception as e:
            logger.error("Ошибка чтения рекомендаций: %s", e)
            return {}

    async def get_recommendations(self) -> dict:
        """Возвращает маппинг рекомендаций."""
        return await asyncio.to_thread(self._sync_get_recommendations)

    @retry_sheets()
    def _sync_update_recommendations_sheet(self, mapping: dict[str, str]) -> None:
        """Обновляет/создаёт записи в листе «Рекомендации» на основе smart rec.

        Сохраняет существующие next_article_link и message, обновляет только
        next_guide_id.
        """
        try:
            ws = self._get_spreadsheet().worksheet(SHEET_RECOMMENDATIONS)
        except gspread.exceptions.WorksheetNotFound:
            spreadsheet = self._get_spreadsheet()
            ws = spreadsheet.add_worksheet(title=SHEET_RECOMMENDATIONS, rows=100, cols=4)
            ws.append_row(
                ["guide_id", "next_guide_id", "next_article_link", "message"],
                value_input_option="USER_ENTERED",
            )
            logger.info("Лист '%s' создан автоматически", SHEET_RECOMMENDATIONS)

        rows = _safe_get_all_records(ws)
        existing: dict[str, int] = {}  # guide_id → row_number (1-indexed, +2 for header)
        header = ws.row_values(1)

        # Находим колонку next_guide_id
        next_col = None
        for i, h in enumerate(header, start=1):
            if h.strip().lower() == "next_guide_id":
                next_col = i
                break

        if next_col is None:
            next_col = 2  # default position

        for idx, row in enumerate(rows, start=2):
            gid = str(row.get("guide_id", row.get("id", ""))).strip()
            if gid:
                existing[gid] = idx

        updated = 0
        created = 0
        for guide_id, next_guide_id in mapping.items():
            if guide_id in existing:
                ws.update_cell(existing[guide_id], next_col, next_guide_id)
                updated += 1
            else:
                ws.append_row(
                    [guide_id, next_guide_id, "", ""],
                    value_input_option="USER_ENTERED",
                )
                created += 1

        logger.info(
            "Recommendations sheet synced: %d updated, %d created",
            updated, created,
        )

    async def update_recommendations_sheet(self, mapping: dict[str, str]) -> None:
        """Обновляет лист Рекомендации из smart rec матрицы."""
        await asyncio.to_thread(self._sync_update_recommendations_sheet, mapping)

    # ── Вопросы пользователей ────────────────────────────────────────────

    @retry_sheets()
    def _sync_append_question(self, row: list) -> None:
        try:
            ws = self._get_spreadsheet().worksheet(SHEET_QUESTIONS)
            ws.append_row(row, value_input_option="USER_ENTERED")
        except gspread.exceptions.WorksheetNotFound:
            spreadsheet = self._get_spreadsheet()
            ws = spreadsheet.add_worksheet(title=SHEET_QUESTIONS, rows=200, cols=8)
            ws.append_row(
                ["Дата", "User ID", "Username", "Имя", "Сфера", "Вопрос", "Ответ", "Статус"],
                value_input_option="USER_ENTERED",
            )
            ws.append_row(row, value_input_option="USER_ENTERED")
            logger.info("Лист '%s' создан автоматически", SHEET_QUESTIONS)

    async def append_question(
        self,
        *,
        user_id: int,
        username: str,
        name: str,
        question: str,
    ) -> None:
        """Сохраняет вопрос пользователя в Sheets."""
        from src.database.crud import get_lead_by_user_id
        lead = await get_lead_by_user_id(user_id)
        sphere = getattr(lead, "business_sphere", "") or "" if lead else ""
        lead_name = lead.name if lead else name

        now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M")
        row = [now, str(user_id), username, lead_name, sphere, question[:500], "", "Новый"]
        await asyncio.to_thread(self._sync_append_question, row)

    @retry_sheets()
    def _sync_update_question_answer(self, question_id: int, answer: str) -> None:
        """Обновляет ответ и статус в листе Вопросы (ищет по тексту ID в строках)."""
        try:
            ws = self._get_spreadsheet().worksheet(SHEET_QUESTIONS)
            rows = _safe_get_all_records(ws)
            header = ws.row_values(1)

            answer_col = None
            status_col = None
            for i, h in enumerate(header, start=1):
                hl = h.strip().lower()
                if hl in ("ответ", "answer"):
                    answer_col = i
                if hl in ("статус", "status"):
                    status_col = i

            if answer_col is None:
                answer_col = 7
            if status_col is None:
                status_col = 8

            # Обновляем последнюю строку со статусом "Новый"
            for idx in range(len(rows) - 1, -1, -1):
                if str(rows[idx].get("Статус", "")).strip() == "Новый":
                    ws.update_cell(idx + 2, answer_col, answer[:500])
                    ws.update_cell(idx + 2, status_col, "Отвечен")
                    break

        except Exception as e:
            logger.error("Error updating question answer in Sheets: %s", e)

    async def update_question_answer(self, question_id: int, answer: str) -> None:
        """Обновляет ответ на вопрос в Sheets."""
        await asyncio.to_thread(self._sync_update_question_answer, question_id, answer)

    # ── Lead Scoring ─────────────────────────────────────────────────────

    @retry_sheets()
    def _sync_update_lead_score(self, user_id: int, score: int, label: str) -> None:
        """Обновляет AI-скоринг лида в Sheets."""
        try:
            ws = self._get_spreadsheet().worksheet(SHEET_LEADS)
            rows = _safe_get_all_records(ws)
            header = ws.row_values(1)

            # Ищем / создаём колонки ai_score и ai_label
            score_col = None
            label_col = None
            for i, h in enumerate(header, start=1):
                if h.lower() == "ai_score":
                    score_col = i
                elif h.lower() == "ai_label":
                    label_col = i

            if score_col is None:
                score_col = len(header) + 1
                ws.update_cell(1, score_col, "ai_score")
                header.append("ai_score")
            if label_col is None:
                label_col = len(header) + 1
                ws.update_cell(1, label_col, "ai_label")

            for idx, row in enumerate(rows, start=2):
                if str(row.get("user_id", "")) == str(user_id):
                    ws.update_cell(idx, score_col, str(score))
                    ws.update_cell(idx, label_col, label)

            logger.info("Lead scoring updated: user_id=%s score=%d label=%s", user_id, score, label)
        except Exception as e:
            logger.error("Lead scoring update error: %s", e)

    async def update_lead_score(self, user_id: int, score: int, label: str) -> None:
        """Асинхронно обновляет скоринг лида."""
        await asyncio.to_thread(self._sync_update_lead_score, user_id, score, label)

    # ── Лог email-кампаний ────────────────────────────────────────────────

    @retry_sheets()
    def _sync_log_email_campaign(self, row: list) -> None:
        """Записывает лог кампании в лист «Email Campaigns»."""
        try:
            spreadsheet = self._get_spreadsheet()
            try:
                ws = spreadsheet.worksheet("Email Campaigns")
            except gspread.exceptions.WorksheetNotFound:
                ws = spreadsheet.add_worksheet(
                    title="Email Campaigns", rows=100, cols=8,
                )
                ws.append_row(
                    ["timestamp", "campaign_id", "segment", "guide_id",
                     "total", "sent", "failed", "status"],
                    value_input_option="USER_ENTERED",
                )
            ws.append_row(row, value_input_option="USER_ENTERED")
            logger.info("Email campaign logged to Sheets")
        except Exception as e:
            logger.error("Email campaign log error: %s", e)

    async def log_email_campaign(
        self,
        *,
        campaign_id: str,
        segment: str,
        guide_id: str,
        total: int,
        sent: int,
        failed: int,
    ) -> None:
        """Логирует email-кампанию в Google Sheets."""
        from datetime import datetime, timezone as tz
        now = datetime.now(tz.utc).strftime("%Y-%m-%d %H:%M:%S")
        status = "OK" if failed == 0 else f"PARTIAL ({failed} errors)"
        row = [now, campaign_id, segment, guide_id, str(total), str(sent), str(failed), status]
        await asyncio.to_thread(self._sync_log_email_campaign, row)
