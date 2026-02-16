"""Обработчик воронки: выбор гайда -> выдача -> сбор контактов -> согласие."""

import asyncio
import logging
import os
import re

from aiogram import Bot, F, Router
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import CallbackQuery, FSInputFile, InlineKeyboardButton, InlineKeyboardMarkup, Message

from src.bot.keyboards.inline import after_guide_keyboard, consent_keyboard, guides_menu_keyboard
from src.bot.utils.cache import TTLCache
from src.bot.utils.compliance import log_consent
from src.bot.utils.disclaimer import add_disclaimer
from src.bot.utils.google_drive import download_guide_pdf
from src.bot.utils.google_sheets import GoogleSheetsClient
from src.bot.utils.scheduler import schedule_followup_series
from src.config import settings
from src.constants import get_text
from src.database.crud import get_lead_by_user_id, save_lead

router = Router()
logger = logging.getLogger(__name__)

# Regex для валидации email
EMAIL_REGEX = re.compile(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$")


# ──────────────────────── FSM States ────────────────────────────────────


class LeadForm(StatesGroup):
    """Состояния формы сбора лидов."""

    waiting_for_email = State()
    waiting_for_name = State()
    consent_given = State()
    waiting_for_business_sphere = State()  # P7: Прогрессивное профилирование


# ──────────────────────── Вспомогательные ───────────────────────────────


def _find_guide(catalog: list[dict], guide_id: str) -> dict | None:
    """Ищет гайд в каталоге по id."""
    for guide in catalog:
        if str(guide.get("id", "")) == guide_id:
            return guide
    return None


# ──────────────────────── Выбор гайда ───────────────────────────────────


@router.callback_query(F.data.startswith("guide_"))
async def process_guide_selection(
    callback: CallbackQuery,
    state: FSMContext,
    bot: Bot,
    google: GoogleSheetsClient,
    cache: TTLCache,
    send_followup=None,
) -> None:
    """Пользователь выбрал гайд — отправляем PDF и начинаем сбор данных.

    Если пользователь уже оставлял данные (повторный пользователь),
    пропускаем форму и сразу отдаём PDF + записываем скачивание.
    """
    guide_id = callback.data.removeprefix("guide_")

    # P5: Телеметрия
    try:
        from src.bot.utils.telemetry import track_event
        asyncio.create_task(track_event(callback.from_user.id, "guide_selected", {"guide": guide_id}))
    except Exception:
        pass

    # Загружаем каталог и тексты из кеша
    catalog = await cache.get_or_fetch("catalog", google.get_guides_catalog)
    texts = await cache.get_or_fetch("texts", google.get_bot_texts)

    guide_info = _find_guide(catalog, guide_id)

    if guide_info is None:
        await callback.answer(
            get_text(texts, "guide_not_found"),
            show_alert=True,
        )
        return

    # Сохраняем выбранный гайд в состояние
    await state.update_data(selected_guide=guide_id)

    await callback.answer()

    # Получаем PDF — из локального хранилища или Google Drive
    file_id = guide_info.get("drive_file_id", "")
    local_path = None
    telegram_file_id = None

    if file_id.startswith("local:"):
        # Локальный гайд (загружен через бота)
        local_guide_id = file_id.removeprefix("local:")
        local_candidate = os.path.join("data", "guides", f"{local_guide_id}.pdf")
        if os.path.isfile(local_candidate):
            local_path = local_candidate
        else:
            # Пробуем отправить по Telegram file_id
            mapping_path = os.path.join("data", "guides", "telegram_files.json")
            if os.path.isfile(mapping_path):
                import json as _json
                with open(mapping_path, "r", encoding="utf-8") as f:
                    mapping = _json.load(f)
                entry = mapping.get(local_guide_id, {})
                telegram_file_id = entry.get("file_id")
    elif file_id:
        local_path = await download_guide_pdf(file_id)

    # Формируем брендированную подпись для PDF
    from src.bot.utils.visual import guide_caption

    guide_title = guide_info.get("title", guide_id)
    guide_desc = guide_info.get("description", "")
    branded_caption = guide_caption(
        title=guide_title,
        description=guide_desc,
    )
    # Telegram caption limit: 1024 chars
    if len(branded_caption) > 1024:
        branded_caption = branded_caption[:1020] + "..."

    if telegram_file_id:
        await callback.message.answer_document(
            document=telegram_file_id,
            caption=branded_caption,
        )
    elif local_path:
        document = FSInputFile(local_path)
        await callback.message.answer_document(
            document=document,
            caption=branded_caption,
        )
    else:
        # PDF не доступен — отправляем текст-заглушку
        await callback.message.answer(
            get_text(
                texts,
                "guide_pdf_unavailable",
                title=guide_title,
                description=guide_desc,
            ),
        )
        logger.warning(
            "PDF не доступен для гайда '%s' (drive_file_id='%s')",
            guide_id,
            file_id,
        )

    # ── Проверяем, повторный ли пользователь ──
    user_id = callback.from_user.id
    existing_lead = await get_lead_by_user_id(user_id)

    if existing_lead:
        # Повторный пользователь — пропускаем форму
        username = callback.from_user.username or ""
        data = await state.get_data()
        traffic_source = data.get("traffic_source", "")

        # Записываем новое скачивание в Sheets
        asyncio.create_task(
            google.append_lead(
                user_id=user_id,
                username=username,
                name=existing_lead.name,
                email=existing_lead.email,
                guide=guide_id,
                source=traffic_source,
            )
        )

        # Планируем follow-up серию
        if send_followup:
            schedule_followup_series(user_id, guide_id, send_followup)

        logger.info(
            "Повторный пользователь user_id=%s скачал гайд '%s' (форма пропущена)",
            user_id,
            guide_id,
        )

        # ── P7: Прогрессивное профилирование ──
        # Если у пользователя ещё нет бизнес-сферы — спрашиваем
        if not getattr(existing_lead, "business_sphere", None):
            await callback.message.answer(
                f"👋 <b>{existing_lead.name}</b>, спасибо за доверие!\n\n"
                "Чтобы мы могли подбирать для вас наиболее релевантные материалы, "
                "подскажите — <b>в какой сфере ваш бизнес?</b>\n\n"
                "Например: IT, строительство, ритейл, финтех, медицина, образование...\n\n"
                "<i>Или отправьте «-» чтобы пропустить</i>",
            )
            await state.update_data(profiling_user_id=user_id)
            await state.set_state(LeadForm.waiting_for_business_sphere)
            return

        # Показываем кнопку "Другие гайды"
        await callback.message.answer(
            get_text(
                texts,
                "returning_user_thanks",
                name=existing_lead.name,
            ),
            reply_markup=after_guide_keyboard(),
        )
        await state.clear()
        return

    # A/B тест текста приглашения email
    from src.bot.utils.growth_engine import get_ab_variant

    variant = get_ab_variant("email_cta", callback.from_user.id)
    if variant == "B":
        email_text = (
            "📝 Оставьте email — мы пришлём:\n"
            "• Обновления законодательства по вашей теме\n"
            "• Приглашения на бесплатные вебинары\n\n"
            "Укажите ваш email:"
        )
    else:
        email_text = get_text(texts, "ask_email")

    await callback.message.answer(email_text)
    await state.set_state(LeadForm.waiting_for_email)


# ──────────────────── P7: Прогрессивное профилирование ─────────────────


@router.message(LeadForm.waiting_for_business_sphere)
async def process_business_sphere(
    message: Message,
    state: FSMContext,
    google: GoogleSheetsClient,
    cache: TTLCache,
) -> None:
    """Сохраняет бизнес-сферу пользователя (прогрессивное профилирование)."""
    text = (message.text or "").strip()

    if text.startswith("/"):
        await state.clear()
        return

    data = await state.get_data()
    user_id = data.get("profiling_user_id", message.from_user.id)

    if text != "-" and len(text) >= 2:
        # Сохраняем бизнес-сферу в БД
        try:
            from src.database.models import Lead, async_session
            from sqlalchemy import select

            async with async_session() as session:
                stmt = (
                    select(Lead)
                    .where(Lead.user_id == user_id)
                    .order_by(Lead.id.desc())
                    .limit(1)
                )
                result = await session.execute(stmt)
                lead = result.scalar_one_or_none()
                if lead:
                    lead.business_sphere = text[:255]
                    await session.commit()
                    logger.info("Business sphere saved: user=%s sphere='%s'", user_id, text[:50])
        except Exception as e:
            logger.error("Failed to save business sphere: %s", e)

        await message.answer(
            f"✅ Спасибо! Записали: <b>{text}</b>\n\n"
            "Теперь мы будем подбирать для вас более релевантный контент.",
            reply_markup=after_guide_keyboard(),
        )

        # Телеметрия
        try:
            from src.bot.utils.telemetry import track_event
            asyncio.create_task(
                track_event(user_id, "business_sphere_entered", {"sphere": text[:50]})
            )
        except Exception:
            pass
    else:
        await message.answer(
            "Хорошо, пропускаем. Вы всегда можете дополнить профиль позже.",
            reply_markup=after_guide_keyboard(),
        )

    await state.clear()


# ──────────────────────── Сбор email ────────────────────────────────────


@router.message(LeadForm.waiting_for_email)
async def process_email(
    message: Message,
    state: FSMContext,
    google: GoogleSheetsClient,
    cache: TTLCache,
) -> None:
    """Валидация и сохранение email."""
    texts = await cache.get_or_fetch("texts", google.get_bot_texts)
    email = message.text.strip() if message.text else ""

    if email.startswith("/"):
        await state.clear()
        return

    if not EMAIL_REGEX.match(email):
        await message.answer(get_text(texts, "invalid_email"))
        return

    # P3: Pydantic-валидация (disposable email, формат)
    from src.bot.utils.validators import validate_lead
    is_valid, err_msg = validate_lead(name="placeholder", email=email)
    if not is_valid and "email" in err_msg.lower():
        await message.answer(f"⚠️ {err_msg}\n\nПопробуйте другой email.")
        return

    await state.update_data(email=email)
    await message.answer(get_text(texts, "email_saved"))

    # Телеметрия
    try:
        from src.bot.utils.telemetry import track_event
        asyncio.create_task(track_event(message.from_user.id, "email_entered"))
    except Exception:
        pass

    await state.set_state(LeadForm.waiting_for_name)


# ──────────────────────── Сбор имени ────────────────────────────────────


@router.message(LeadForm.waiting_for_name)
async def process_name(
    message: Message,
    state: FSMContext,
    google: GoogleSheetsClient,
    cache: TTLCache,
) -> None:
    """Сохранение имени и запрос согласия."""
    texts = await cache.get_or_fetch("texts", google.get_bot_texts)
    name = message.text.strip() if message.text else ""

    if name.startswith("/"):
        await state.clear()
        return

    if len(name) < 2:
        await message.answer(get_text(texts, "invalid_name"))
        return

    # P3: Pydantic-валидация имени (мусорный текст, только цифры)
    from src.bot.utils.validators import is_garbage_text
    if is_garbage_text(name):
        await message.answer(
            "Пожалуйста, введите настоящее имя (минимум 2 символа, не тестовый текст)."
        )
        return

    await state.update_data(name=name)

    # Телеметрия
    try:
        from src.bot.utils.telemetry import track_event
        asyncio.create_task(track_event(message.from_user.id, "name_entered"))
    except Exception:
        pass

    # Показываем согласие на обработку данных
    await message.answer(
        get_text(texts, "consent_text", privacy_url=settings.PRIVACY_POLICY_URL),
        reply_markup=consent_keyboard(),
        disable_web_page_preview=True,
    )
    await state.set_state(LeadForm.consent_given)


# ──────────────────────── Согласие ──────────────────────────────────────


@router.callback_query(F.data == "give_consent", LeadForm.consent_given)
async def process_consent(
    callback: CallbackQuery,
    state: FSMContext,
    bot: Bot,
    google: GoogleSheetsClient,
    cache: TTLCache,
    send_followup=None,
) -> None:
    """Пользователь дал согласие — сохраняем лид."""
    data = await state.get_data()
    texts = await cache.get_or_fetch("texts", google.get_bot_texts)

    user_id = callback.from_user.id
    username = callback.from_user.username or ""
    email = data.get("email", "")
    name = data.get("name", "")
    selected_guide = data.get("selected_guide", "")
    traffic_source = data.get("traffic_source", "")

    # 0. Записываем A/B конверсию
    try:
        from src.bot.utils.growth_engine import record_ab_conversion
        record_ab_conversion("email_cta", user_id)
    except Exception:
        pass

    # 1. Сохраняем лид в SQLite (надёжный backup)
    await save_lead(
        user_id=user_id,
        email=email,
        name=name,
        selected_guide=selected_guide,
    )

    # 2. Записываем лид в Google Sheets (CRM для менеджеров)
    asyncio.create_task(
        google.append_lead(
            user_id=user_id,
            username=username,
            name=name,
            email=email,
            guide=selected_guide,
            source=traffic_source,
        )
    )

    # 3. Логируем согласие для compliance
    await log_consent(user_id=user_id, consent_type="personal_data_processing")

    # P5: Телеметрия — лид сохранён
    try:
        from src.bot.utils.telemetry import track_event
        asyncio.create_task(track_event(user_id, "consent_given"))
        asyncio.create_task(track_event(user_id, "lead_saved", {"guide": selected_guide}))
    except Exception:
        pass

    logger.info(
        "Новый лид: user_id=%s, email=%s, name=%s, guide=%s",
        user_id,
        email,
        name,
        selected_guide,
    )

    # C4: Отправляем приветственный email
    try:
        from src.bot.utils.email_sender import send_welcome_email
        asyncio.create_task(send_welcome_email(name, email, selected_guide))
    except Exception:
        pass  # non-critical

    # 4. Планируем follow-up серию сообщений
    if send_followup and selected_guide:
        schedule_followup_series(user_id, selected_guide, send_followup)

    # 5. Благодарим пользователя + кнопка "Другие гайды"
    await callback.message.edit_text(
        get_text(texts, "consent_given", name=name, email=email),
    )
    await callback.message.answer(
        "📚 Хотите посмотреть другие полезные материалы?",
        reply_markup=after_guide_keyboard(),
    )

    # L4: Conflict Check — проверяем нового клиента на конфликт интересов
    try:
        from src.bot.utils.legal_search import check_conflicts

        async def _conflict_check():
            result = await check_conflicts(name=name, google=google)
            if result.get("has_conflict"):
                conflict_text = (
                    f"⚠️ <b>Conflict Check Alert</b>\n\n"
                    f"Новый лид: {name} ({email})\n"
                    f"Риск: {result['risk_level']}\n"
                    f"Совпадений: {len(result['matches'])}\n\n"
                )
                for m in result["matches"][:5]:
                    conflict_text += f"  • {m['type']}: {m.get('name', '')} ({m['match_term']})\n"
                await bot.send_message(settings.ADMIN_ID, conflict_text)

        asyncio.create_task(_conflict_check())
    except Exception:
        pass  # non-critical

    # 6. Уведомляем администратора (с контекстом и кнопками)
    asyncio.create_task(
        notify_admin(
            bot,
            user_id=user_id,
            username=username,
            name=name,
            email=email,
            guide=selected_guide,
            source=traffic_source,
        )
    )

    # 7. Сброс состояния
    await state.clear()
    await callback.answer()


@router.callback_query(F.data == "decline_consent", LeadForm.consent_given)
async def process_decline(
    callback: CallbackQuery,
    state: FSMContext,
    google: GoogleSheetsClient,
    cache: TTLCache,
) -> None:
    """Пользователь отказался от обработки данных."""
    texts = await cache.get_or_fetch("texts", google.get_bot_texts)
    await callback.message.edit_text(get_text(texts, "consent_declined"))
    await state.clear()
    await callback.answer()


# ──────────────────────── Кнопка «Все гайды» ─────────────────────────────


@router.callback_query(F.data == "show_all_guides")
async def show_all_guides(
    callback: CallbackQuery,
    google: GoogleSheetsClient,
    cache: TTLCache,
) -> None:
    """Показывает каталог гайдов по нажатию кнопки 'Посмотреть другие гайды'."""
    catalog = await cache.get_or_fetch("catalog", google.get_guides_catalog)
    texts = await cache.get_or_fetch("texts", google.get_bot_texts)

    if not catalog:
        await callback.answer("Каталог пуст. Попробуйте позже.", show_alert=True)
        return

    await callback.message.answer(
        get_text(texts, "welcome_subscribed"),
        reply_markup=guides_menu_keyboard(catalog),
    )
    await callback.answer()


# ──────────────────────── Вспомогательные ───────────────────────────────


def _esc_html(text: str) -> str:
    """Экранирует спецсимволы HTML в пользовательских данных."""
    return (
        text.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
    )


async def notify_admin(
    bot: Bot,
    *,
    user_id: int,
    username: str,
    name: str,
    email: str,
    guide: str,
    source: str = "",
) -> None:
    """Отправка уведомления администратору о новом лиде с контекстом и кнопками."""
    try:
        source_line = f"📍 Источник: {_esc_html(source)}\n" if source else ""
        username_display = f"@{username}" if username else "нет"

        text = (
            "🆕 <b>Новый лид!</b>\n\n"
            f"👤 Имя: {_esc_html(name)}\n"
            f"📧 Email: {_esc_html(email)}\n"
            f"📚 Гайд: {_esc_html(guide)}\n"
            f"💬 Telegram: {username_display}\n"
            f"{source_line}"
            f"🆔 User ID: <code>{user_id}</code>"
        )

        # Кнопки для быстрых действий
        crm_url = (
            f"https://docs.google.com/spreadsheets/d/"
            f"{settings.GOOGLE_SPREADSHEET_ID}/edit#gid=0"
        )
        buttons = [
            [
                InlineKeyboardButton(text="📊 Открыть CRM", url=crm_url),
            ],
            [
                InlineKeyboardButton(
                    text=f"✉️ Написать email",
                    url=f"mailto:{email}",
                ),
            ],
        ]
        # Если есть username — добавить кнопку для написания в Telegram
        if username:
            buttons.append(
                [
                    InlineKeyboardButton(
                        text=f"💬 Написать в Telegram",
                        url=f"https://t.me/{username}",
                    ),
                ]
            )

        keyboard = InlineKeyboardMarkup(inline_keyboard=buttons)

        await bot.send_message(
            chat_id=settings.ADMIN_ID,
            text=text,
            reply_markup=keyboard,
        )
    except Exception as e:
        logger.error("Не удалось уведомить администратора: %s", e)
