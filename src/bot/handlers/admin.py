"""Admin-команды бота (только для ADMIN_ID)."""

import logging
from datetime import datetime, timezone, timedelta

from aiogram import Router
from aiogram.filters import Command
from aiogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton

from src.bot.utils.cache import TTLCache
from src.bot.utils.google_drive import clear_pdf_cache
from src.bot.utils.google_sheets import GoogleSheetsClient
from src.config import settings
from src.constants import get_text

router = Router()
logger = logging.getLogger(__name__)


@router.message(Command("refresh"))
async def cmd_refresh(message: Message, cache: TTLCache) -> None:
    """Сброс кеша — бот подтянет свежие данные из Google Sheets.

    Доступно только администратору (ADMIN_ID).
    Также очищает локальный кеш PDF-файлов.
    """
    if message.from_user is None or message.from_user.id != settings.ADMIN_ID:
        return

    # Сбрасываем кеш текстов и каталога
    cache.invalidate()

    # Очищаем кеш скачанных PDF
    pdf_count = clear_pdf_cache()

    logger.info(
        "Кеш сброшен администратором (user_id=%s), PDF удалено: %d",
        message.from_user.id,
        pdf_count,
    )

    await message.answer(
        f"✅ Кеш сброшен.\n"
        f"• Тексты и каталог обновятся при следующем запросе\n"
        f"• PDF-кеш очищен ({pdf_count} файлов)"
    )


# ═══════════════════════════════════════════════════════════════════════════
#  /report — Dashboard-отчёт за 24 часа
# ═══════════════════════════════════════════════════════════════════════════


@router.message(Command("report"))
async def cmd_report(
    message: Message,
    google: GoogleSheetsClient,
    cache: TTLCache,
) -> None:
    """24-часовой dashboard: лиды, ошибки API, популярные темы, AI-резюме."""
    if message.from_user is None or message.from_user.id != settings.ADMIN_ID:
        return

    await message.answer("📊 Собираю данные за 24 часа...")

    try:
        now = datetime.now(timezone.utc)
        today_str = now.strftime("%d.%m.%Y")

        # 1. Лиды за сегодня
        leads = await google.get_recent_leads(limit=100)
        today_leads = [
            l for l in leads
            if l.get("timestamp", "").startswith(today_str)
            or l.get("timestamp", "")[:10] == now.strftime("%Y-%m-%d")
        ]
        total_leads = len(today_leads)

        # 2. Консультации за сегодня
        consult_log = await google.get_consult_log(limit=200)
        today_consults = [
            c for c in consult_log
            if c.get("timestamp", c.get("Дата", "")).startswith(today_str)
            or c.get("timestamp", c.get("Дата", ""))[:10] == now.strftime("%Y-%m-%d")
        ]
        total_consults = len(today_consults)

        # 3. Популярные темы (из последних консультаций)
        questions = [
            c.get("question", c.get("Вопрос", ""))[:80]
            for c in today_consults
            if c.get("question", c.get("Вопрос", ""))
        ]

        # 4. AI Rate Limit статистика
        try:
            from src.bot.middlewares.rate_limit import get_total_ai_usage_today
            ai_stats = get_total_ai_usage_today()
        except Exception:
            ai_stats = {"total_requests": 0, "unique_users": 0}

        # 5. DB stats
        try:
            from src.database.models import async_session, User
            from sqlalchemy import select, func as sa_func

            async with async_session() as session:
                total_users = (await session.execute(
                    select(sa_func.count(User.id))
                )).scalar() or 0

                active_24h = (await session.execute(
                    select(sa_func.count(User.id)).where(
                        User.last_activity >= now - timedelta(hours=24)
                    )
                )).scalar() or 0
        except Exception:
            total_users = 0
            active_24h = 0

        # 6. Формируем отчёт (HTML + progress bars)
        from src.bot.utils.visual import progress_bar, stat_card, format_number

        # Целевые значения для progress bars
        leads_target = max(10, total_leads + 5)
        consults_target = max(20, total_consults + 10)

        report = (
            f"📊 <b>Dashboard-отчёт за {today_str}</b>\n"
            f"{'─' * 30}\n\n"
        )

        # Пользователи — карточка
        report += stat_card("Пользователи", {
            "Всего в базе": total_users,
            "Активных за 24ч": active_24h,
        }, emoji="👥") + "\n\n"

        # Лиды — progress bar
        report += (
            f"🔥 <b>Лиды:</b>\n"
            f"<code>{progress_bar(total_leads, leads_target, label='Сегодня')}</code>\n\n"
        )

        # AI — progress bar
        report += (
            f"🤖 <b>AI-консультации:</b>\n"
            f"<code>{progress_bar(total_consults, consults_target, label='Вопросы')}</code>\n"
            f"<code>{progress_bar(ai_stats['total_requests'], max(50, ai_stats['total_requests'] + 20), label='API')}</code>\n"
            f"• Уникальных юзеров: <b>{ai_stats['unique_users']}</b>\n\n"
        )

        # Топ вопросы
        if questions:
            report += "💬 <b>Последние вопросы:</b>\n"
            for q in questions[:5]:
                report += f"  • <i>{q}</i>\n"
            report += "\n"

        # 7. AI-резюме дня
        try:
            from src.bot.utils.ai_client import ask_digest

            summary_prompt = (
                f"Данные бота за сегодня:\n"
                f"- Новых лидов: {total_leads}\n"
                f"- AI-консультаций: {total_consults}\n"
                f"- Активных пользователей: {active_24h}\n"
                f"- AI-запросов: {ai_stats['total_requests']}\n"
            )

            if questions:
                summary_prompt += f"- Последние вопросы: {'; '.join(questions[:5])}\n"

            summary_prompt += (
                "\nНапиши краткое резюме дня (2-3 предложения):\n"
                "- Общая оценка активности\n"
                "- Тренды (что спрашивают чаще)\n"
                "- Рекомендация по контенту"
            )

            ai_summary = await ask_digest(summary_prompt, max_tokens=512)
            report += f"🧠 <b>AI-резюме:</b>\n{ai_summary}\n"

        except Exception as e:
            report += f"🧠 AI-резюме: недоступно ({e})\n"

        # Кнопки
        keyboard = InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(
                    text="📊 Открыть CRM",
                    url=f"https://docs.google.com/spreadsheets/d/{settings.GOOGLE_SPREADSHEET_ID}/edit",
                )],
                [InlineKeyboardButton(
                    text="✅ Принято",
                    callback_data="digest_ack",
                )],
            ]
        )

        try:
            await message.answer(report, reply_markup=keyboard)
        except Exception:
            await message.answer(report, reply_markup=keyboard, parse_mode=None)

        logger.info("Report generated for admin")

    except Exception as e:
        logger.error("Report generation error: %s", e)
        await message.answer(f"❌ Ошибка генерации отчёта: {e}")


# ═══════════════════════════════════════════════════════════════════════════
#  /growth — Growth Hacker отчёт (вручную)
# ═══════════════════════════════════════════════════════════════════════════


@router.message(Command("growth"))
async def cmd_growth(
    message: Message,
    google: GoogleSheetsClient,
    cache: TTLCache,
) -> None:
    """Ручной запуск Growth Hacker Report."""
    if message.from_user is None or message.from_user.id != settings.ADMIN_ID:
        return

    await message.answer("📈 Генерирую Growth Report...")

    try:
        from src.bot.utils.growth_report import send_growth_report
        await send_growth_report(bot=message.bot, google=google, cache=cache)
    except Exception as e:
        logger.error("Growth report error: %s", e)
        await message.answer(f"❌ Ошибка: {e}")


# ═══════════════════════════════════════════════════════════════════════════
#  /audit — Аудит безопасности (P10)
# ═══════════════════════════════════════════════════════════════════════════


@router.message(Command("audit"))
async def cmd_audit(message: Message) -> None:
    """Запускает OWASP-аудит безопасности кодовой базы."""
    if message.from_user is None or message.from_user.id != settings.ADMIN_ID:
        return

    await message.answer("🔒 Запускаю аудит безопасности...")

    try:
        from src.bot.utils.security_audit import run_security_audit, format_audit_report
        audit = run_security_audit()
        report = format_audit_report(audit)

        try:
            await message.answer(report)
        except Exception:
            await message.answer(report, parse_mode=None)
    except Exception as e:
        logger.error("Security audit error: %s", e)
        await message.answer(f"❌ Ошибка аудита: {e}")


# ═══════════════════════════════════════════════════════════════════════════
#  /funnel — Воронка продаж (P5)
# ═══════════════════════════════════════════════════════════════════════════


@router.message(Command("funnel"))
async def cmd_funnel(
    message: Message,
    google: GoogleSheetsClient,
    cache: TTLCache,
) -> None:
    """Показывает воронку продаж с AI-анализом."""
    if message.from_user is None or message.from_user.id != settings.ADMIN_ID:
        return

    await message.answer("📊 Анализирую воронку продаж...")

    try:
        from src.bot.utils.telemetry import analyze_funnel
        analysis = await analyze_funnel(ai_client=True)

        text = f"📈 <b>Воронка продаж</b>\n\n{analysis}"

        try:
            await message.answer(text[:4000])
        except Exception:
            await message.answer(text[:4000], parse_mode=None)
    except Exception as e:
        logger.error("Funnel analysis error: %s", e)
        await message.answer(f"❌ Ошибка анализа: {e}")


# ═══════════════════════════════════════════════════════════════════════════
#  /errors — Статистика ошибок (P1)
# ═══════════════════════════════════════════════════════════════════════════


@router.message(Command("errors"))
async def cmd_errors(message: Message) -> None:
    """Показывает статистику ошибок."""
    if message.from_user is None or message.from_user.id != settings.ADMIN_ID:
        return

    from src.bot.middlewares.error_handler import get_error_stats
    stats = get_error_stats()

    if not stats:
        await message.answer("✅ Ошибок пока не зафиксировано!")
        return

    lines = ["🚨 <b>Статистика ошибок</b>\n"]
    for exc_type, count in sorted(stats.items(), key=lambda x: -x[1]):
        lines.append(f"  • <code>{exc_type}</code>: {count}")

    total = sum(stats.values())
    lines.append(f"\n📊 Всего: {total}")

    await message.answer("\n".join(lines))
