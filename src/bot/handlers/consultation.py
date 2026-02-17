"""Запись на бесплатную мини-консультацию.

Flow:
    1. Кнопка «Записаться на консультацию» (после гайда или из меню)
    2. Бот запрашивает номер телефона
    3. Бот запрашивает удобное время
    4. Сохраняем заявку → уведомляем админа
"""

import asyncio
import logging

from aiogram import Bot, F, Router
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
)

from src.bot.keyboards.inline import after_guide_keyboard
from src.bot.utils.cache import TTLCache
from src.bot.utils.google_sheets import GoogleSheetsClient
from src.bot.utils.throttle import critical_limiter
from src.config import settings
from src.database.crud import get_lead_by_user_id, track, update_lead_sphere

router = Router()
logger = logging.getLogger(__name__)


class ConsultForm(StatesGroup):
    """FSM для записи на консультацию."""

    waiting_for_sphere = State()
    waiting_for_phone = State()
    waiting_for_time = State()


# ── Точки входа ──────────────────────────────────────────────────────


@router.callback_query(F.data == "book_consultation")
async def start_consultation_cb(callback: CallbackQuery, state: FSMContext) -> None:
    """Начало записи через inline-кнопку."""
    await callback.answer()
    await _ask_phone(callback.message, state, callback.from_user.id)


@router.message(Command("consultation"))
async def start_consultation_cmd(message: Message, state: FSMContext) -> None:
    """Начало записи через команду /consultation."""
    await _ask_phone(message, state, message.from_user.id)


@router.message(F.text == "📞 Консультация")
async def start_consultation_menu(message: Message, state: FSMContext) -> None:
    """Начало записи через ReplyKeyboard."""
    await _ask_phone(message, state, message.from_user.id)


async def _ask_phone(message: Message, state: FSMContext, user_id: int) -> None:
    """Запрашивает номер телефона (или сначала сферу, если не указана)."""
    if not critical_limiter.allow(user_id, "consultation"):
        await message.answer("⏳ Вы недавно уже подавали заявку. Подождите немного.")
        return

    lead = await get_lead_by_user_id(user_id)
    greeting = f"{lead.name}, давайте " if lead else "Давайте "

    await state.clear()

    # Если сфера бизнеса не указана — обязательно спрашиваем
    if lead and not getattr(lead, "business_sphere", None):
        from src.bot.utils.profiling import PROFILE_QUESTIONS, build_question_keyboard
        sphere_q = next((q for q in PROFILE_QUESTIONS if q.field == "business_sphere"), None)
        if sphere_q:
            kb = build_question_keyboard(sphere_q)
            kb.inline_keyboard.append(
                [InlineKeyboardButton(text="Отмена", callback_data="cancel_consultation")]
            )
        else:
            kb = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="Отмена", callback_data="cancel_consultation")],
            ])
        await state.set_state(ConsultForm.waiting_for_sphere)
        await state.update_data(return_to="consultation")
        await message.answer(
            f"{lead.name}, перед записью — подскажите, "
            "<b>в какой сфере ваш бизнес?</b>\n\n"
            "Это поможет юристу подготовиться к разговору.",
            reply_markup=kb,
        )
        return

    await state.set_state(ConsultForm.waiting_for_phone)

    # Social proof с учётом сферы
    sphere = getattr(lead, "business_sphere", None) or "" if lead else ""
    case_line = ""
    consult_pitch = ""
    if sphere:
        from src.bot.handlers.lead_form import SPHERE_CASES, _normalize_sphere
        norm = _normalize_sphere(sphere)
        case = SPHERE_CASES.get(norm)
        if case:
            case_line = f"\n\n💼 <i>{case}</i>"
            consult_pitch = (
                f"\n\nОбсудим вашу ситуацию — как мы уже делали "
                f"с компаниями из сферы «{sphere}»."
            )
    if not case_line:
        case_line = (
            "\n\n✅ <i>Наши юристы провели 300+ консультаций "
            "для бизнеса в Казахстане — обсудим вашу ситуацию, "
            "как мы делали с десятками компаний.</i>"
        )

    # Urgency: дефицит слотов
    from src.bot.handlers.lead_form import _get_consult_scarcity_line
    scarcity = await _get_consult_scarcity_line()
    scarcity_line = f"\n\n{scarcity}" if scarcity else ""

    await message.answer(
        f"📞 {greeting}назначим короткую консультацию "
        "с нашим юристом.\n\n"
        "Это <b>бесплатно</b> и ни к чему не обязывает — "
        f"просто обсудим ваш вопрос за 15 минут.{case_line}"
        f"{consult_pitch}"
        f"{scarcity_line}\n\n"
        "Укажите ваш номер телефона:",
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="Отмена", callback_data="cancel_consultation")],
            ]
        ),
    )


# ── Сбор сферы (перед консультацией) ──────────────────────────────────


@router.callback_query(F.data == "consult_skip_sphere", ConsultForm.waiting_for_sphere)
async def skip_sphere_consult(callback: CallbackQuery, state: FSMContext) -> None:
    """Сфера обязательна — мягко просим выбрать."""
    await callback.answer()
    await callback.message.answer(
        "Пожалуйста, выберите сферу — это поможет юристу "
        "подготовиться к разговору.",
    )


@router.callback_query(
    F.data.startswith("profile_business_sphere_"),
    ConsultForm.waiting_for_sphere,
)
async def process_sphere_button_consult(
    callback: CallbackQuery,
    state: FSMContext,
    google: GoogleSheetsClient,
) -> None:
    """Обработка выбора сферы через inline-кнопку перед консультацией."""
    value = callback.data.removeprefix("profile_business_sphere_")
    await callback.answer()

    if value == "skip":
        await callback.message.answer(
            "Пожалуйста, выберите сферу — это поможет юристу "
            "подготовиться к разговору.",
        )
        return

    user_id = callback.from_user.id
    await update_lead_sphere(user_id, value)
    asyncio.create_task(google.update_lead_sphere(user_id, value))
    from src.database.crud import update_user_profile
    await update_user_profile(user_id, business_sphere=value)
    logger.info("Sphere (consult button): user=%s sphere='%s'", user_id, value)

    await state.set_state(ConsultForm.waiting_for_phone)
    await callback.message.edit_text(
        f"👍 Отлично, {value}! Теперь укажите ваш номер телефона:",
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="Отмена", callback_data="cancel_consultation")],
            ]
        ),
    )


@router.message(ConsultForm.waiting_for_sphere)
async def process_sphere_consult(
    message: Message,
    state: FSMContext,
    google: GoogleSheetsClient,
) -> None:
    """Сохраняет сферу и переходит к телефону."""
    text = (message.text or "").strip()

    if text.startswith("/"):
        await state.clear()
        return

    if len(text) >= 2:
        user_id = message.from_user.id
        sphere = text[:100]
        await update_lead_sphere(user_id, sphere)
        asyncio.create_task(google.update_lead_sphere(user_id, sphere))
        from src.database.crud import update_user_profile
        await update_user_profile(user_id, business_sphere=sphere)
        logger.info("Sphere (consult): user=%s sphere='%s'", user_id, sphere[:50])

    await state.set_state(ConsultForm.waiting_for_phone)
    await message.answer(
        "👍 Спасибо! Теперь укажите ваш номер телефона:",
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="Отмена", callback_data="cancel_consultation")],
            ]
        ),
    )


# ── Сбор телефона ────────────────────────────────────────────────────


@router.message(ConsultForm.waiting_for_phone)
async def process_phone(message: Message, state: FSMContext) -> None:
    """Сохраняем телефон, просим удобное время."""
    text = (message.text or "").strip()

    if text.startswith("/"):
        await state.clear()
        return

    # Базовая валидация: минимум 7 цифр
    digits = "".join(c for c in text if c.isdigit())
    if len(digits) < 7:
        await message.answer(
            "Пожалуйста, укажите корректный номер.\n"
            "Пример: <code>+7 777 123 45 67</code>"
        )
        return

    await state.update_data(phone=text)
    await state.set_state(ConsultForm.waiting_for_time)
    await message.answer(
        "Отлично, записали.\n\n"
        "Когда вам удобно принять звонок?\n"
        "<i>Например: «сегодня после 15:00» или «завтра утром»</i>",
    )


# ── Сбор времени ─────────────────────────────────────────────────────


@router.message(ConsultForm.waiting_for_time)
async def process_time(
    message: Message,
    state: FSMContext,
    bot: Bot,
    google: GoogleSheetsClient,
    cache: TTLCache,
) -> None:
    """Финализируем заявку: сохраняем и уведомляем админа."""
    text = (message.text or "").strip()

    if text.startswith("/"):
        await state.clear()
        return

    if len(text) < 3:
        await message.answer("Укажите удобное время подробнее:")
        return

    data = await state.get_data()
    phone = data.get("phone", "")
    user_id = message.from_user.id
    username = message.from_user.username or ""
    full_name = message.from_user.full_name or ""

    # Имя и email из лида (если есть)
    lead = await get_lead_by_user_id(user_id)
    lead_name = lead.name if lead else full_name
    lead_email = lead.email if lead else "не указан"

    await state.clear()

    # Благодарим пользователя
    await message.answer(
        "✅ <b>Заявка принята!</b>\n\n"
        f"📞 Телефон: {phone}\n"
        f"🕐 Время: {text}\n\n"
        "Наш юрист свяжется с вами в указанное время.\n"
        "А пока — посмотрите другие гайды в нашей библиотеке.",
        reply_markup=after_guide_keyboard(),
    )

    # Воронка: запись на консультацию
    asyncio.create_task(track(user_id, "consultation"))

    # Трекаем запись на консультацию (для подсчёта слотов)
    from src.database.crud import save_lead as _save_lead_consult
    try:
        await _save_lead_consult(
            user_id=user_id,
            email=lead_email,
            name=lead_name,
            selected_guide="__consultation__",
        )
    except Exception:
        pass

    # Записываем в Google Sheets (лист Консультации, если есть)
    try:
        await google.append_consultation(
            user_id=user_id,
            username=username,
            name=lead_name,
            email=lead_email,
            phone=phone,
            preferred_time=text,
        )
    except Exception as e:
        logger.warning("Sheets consultation save failed: %s", e)

    # Уведомляем админа
    asyncio.create_task(
        _notify_admin_consultation(
            bot,
            user_id=user_id,
            username=username,
            name=lead_name,
            email=lead_email,
            phone=phone,
            preferred_time=text,
        )
    )

    logger.info("Consultation booked: user=%s, phone=%s, time=%s", user_id, phone, text)


# ── Отмена ───────────────────────────────────────────────────────────


@router.callback_query(F.data == "cancel_consultation")
async def cancel_consultation(callback: CallbackQuery, state: FSMContext) -> None:
    await state.clear()
    await callback.message.edit_text(
        "Хорошо, если передумаете — нажмите /consultation или кнопку «📞 Консультация».",
    )
    await callback.answer()


# ── Уведомление админа ───────────────────────────────────────────────


def _esc(text: str) -> str:
    return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


async def _notify_admin_consultation(
    bot: Bot,
    *,
    user_id: int,
    username: str,
    name: str,
    email: str,
    phone: str,
    preferred_time: str,
) -> None:
    """Отправляет админу уведомление о новой заявке на консультацию."""
    from datetime import datetime, timezone

    try:
        username_display = f"@{username}" if username else "нет"
        now = datetime.now(timezone.utc).strftime("%d.%m.%Y %H:%M UTC")

        # Подтягиваем сферу
        sphere_line = ""
        try:
            lead = await get_lead_by_user_id(user_id)
            if lead and getattr(lead, "business_sphere", None):
                sphere_line = f"🏢 Сфера: {_esc(lead.business_sphere)}\n"
        except Exception:
            pass

        text = (
            "📞 <b>Новая заявка на консультацию!</b>\n\n"
            f"👤 Имя: {_esc(name)}\n"
            f"📧 Email: {_esc(email)}\n"
            f"📱 Телефон: <code>{_esc(phone)}</code>\n"
            f"🕐 Удобное время: {_esc(preferred_time)}\n"
            f"💬 Telegram: {username_display}\n"
            f"{sphere_line}"
            f"📅 Заявка: {now}\n"
            f"🆔 User ID: <code>{user_id}</code>"
        )

        buttons = []
        if username:
            buttons.append(
                [InlineKeyboardButton(text="💬 Написать в Telegram", url=f"https://t.me/{username}")]
            )
        buttons.append(
            [InlineKeyboardButton(
                text="📊 Открыть CRM",
                url=f"https://docs.google.com/spreadsheets/d/{settings.GOOGLE_SPREADSHEET_ID}/edit",
            )]
        )

        await bot.send_message(
            chat_id=settings.ADMIN_ID,
            text=text,
            reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
        )
    except Exception as e:
        logger.error("Admin notification failed: %s", e)
