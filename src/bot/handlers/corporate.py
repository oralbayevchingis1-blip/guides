"""Корпоративный стандарт — обработчики.

C1: /booking — запись на консультацию (Google Calendar)
C2: /docgen — генерация документов по шаблонам (.docx)
C3: /mytasks — задачи клиента (Legal Task Tracker)
C6: /invoice — выставление счёта за документы
"""

import asyncio
import logging
import os
from pathlib import Path

from aiogram import Bot, F, Router
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import (
    CallbackQuery,
    FSInputFile,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
)

from src.bot.utils.cache import TTLCache
from src.bot.utils.google_sheets import GoogleSheetsClient
from src.config import settings

router = Router()
logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════════════════
#  FSM States
# ═══════════════════════════════════════════════════════════════════════════

class BookingStates(StatesGroup):
    choosing_slot = State()
    entering_topic = State()
    entering_contact = State()

class DocGenStates(StatesGroup):
    choosing_template = State()
    answering_questions = State()

class TaskStates(StatesGroup):
    viewing = State()


# ═══════════════════════════════════════════════════════════════════════════
#  C1: /booking — запись на консультацию
# ═══════════════════════════════════════════════════════════════════════════


@router.message(Command("booking"))
async def cmd_booking(message: Message, state: FSMContext) -> None:
    """Показывает доступные слоты для записи."""
    await message.answer("📅 Загружаю доступные слоты...")

    from src.bot.utils.calendar_client import get_available_slots

    slots = await get_available_slots(days_ahead=5)

    if not slots:
        await message.answer(
            "😔 К сожалению, в ближайшие дни нет свободных слотов.\n\n"
            "📞 Свяжитесь с нами напрямую:",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="📞 Написать юристу", url="https://t.me/SOLISlegal")],
            ]),
        )
        return

    # Формируем кнопки слотов (макс 8)
    buttons = []
    for slot in slots[:8]:
        buttons.append([InlineKeyboardButton(
            text=f"📅 {slot['display']}",
            callback_data=f"book_{slot['id']}",
        )])
    buttons.append([InlineKeyboardButton(text="❌ Отмена", callback_data="booking_cancel")])

    await message.answer(
        "📅 <b>Запись на консультацию SOLIS Partners</b>\n\n"
        "Выберите удобное время:\n"
        "<i>Длительность: 30 минут</i>\n\n"
        "⚖️ <i>Время указано по Алматы (UTC+5)</i>",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
    )
    await state.set_state(BookingStates.choosing_slot)


@router.callback_query(F.data.startswith("book_"))
async def select_slot(callback: CallbackQuery, state: FSMContext) -> None:
    """Выбор слота и запрос темы консультации."""
    slot_id = callback.data.removeprefix("book_")
    await state.update_data(slot_id=slot_id)
    await state.set_state(BookingStates.entering_topic)

    await callback.message.answer(
        "✅ Слот выбран!\n\n"
        "📝 Кратко опишите тему консультации\n"
        "(или отправьте <code>-</code> для «Общая консультация»):"
    )
    await callback.answer()


@router.message(BookingStates.entering_topic)
async def enter_booking_topic(message: Message, state: FSMContext) -> None:
    topic = (message.text or "").strip()
    if topic == "-":
        topic = "Юридическая консультация"
    await state.update_data(topic=topic)
    await state.set_state(BookingStates.entering_contact)

    await message.answer(
        "📧 Ваш email для подтверждения\n"
        "(или <code>-</code> чтобы пропустить):"
    )


@router.message(BookingStates.entering_contact)
async def confirm_booking(message: Message, state: FSMContext, bot: Bot) -> None:
    """Финальное подтверждение и создание события."""
    email = (message.text or "").strip()
    if email == "-":
        email = ""

    data = await state.get_data()
    await state.clear()

    from src.bot.utils.calendar_client import create_event

    user = message.from_user
    client_name = user.full_name or user.username or f"User {user.id}"

    result = await create_event(
        slot_id=data["slot_id"],
        client_name=client_name,
        client_email=email,
        topic=data.get("topic", "Консультация"),
    )

    if result["success"]:
        slot = result["slot"]
        await message.answer(
            f"✅ <b>Консультация забронирована!</b>\n\n"
            f"📅 Дата: {slot['date']}\n"
            f"🕐 Время: {slot['time']} — {slot['end_time']} (Алматы)\n"
            f"📝 Тема: {data.get('topic', '—')}\n"
            f"{'📧 Email: ' + email if email else ''}\n\n"
            f"⚖️ <i>Юрист SOLIS Partners свяжется с вами.\n"
            f"Если нужно перенести — напишите @SOLISlegal</i>",
        )

        # Уведомляем админа
        try:
            admin_text = (
                f"📅 <b>Новая запись на консультацию</b>\n\n"
                f"👤 {client_name} (ID: {user.id})\n"
                f"📧 {email or '—'}\n"
                f"📅 {slot['date']} {slot['time']}—{slot['end_time']}\n"
                f"📝 Тема: {data.get('topic', '—')}"
            )
            await bot.send_message(settings.ADMIN_ID, admin_text)
        except Exception:
            pass

        # Карма за бронирование
        try:
            from src.bot.utils.karma import add_karma
            add_karma(user.id, 5, "booking")
        except Exception:
            pass
    else:
        await message.answer(
            f"❌ {result.get('error', 'Ошибка бронирования')}\n\n"
            "Попробуйте выбрать другой слот: /booking",
        )


@router.callback_query(F.data == "booking_cancel")
async def cancel_booking(callback: CallbackQuery, state: FSMContext) -> None:
    await state.clear()
    await callback.message.answer("❌ Запись отменена.")
    await callback.answer()


# ═══════════════════════════════════════════════════════════════════════════
#  C2: /docgen — генерация документов по шаблонам
# ═══════════════════════════════════════════════════════════════════════════


@router.message(Command("docgen"))
async def cmd_docgen(message: Message, state: FSMContext) -> None:
    """Каталог шаблонов для генерации .docx."""
    from src.bot.utils.docx_engine import DOCX_TEMPLATES

    buttons = []
    for tmpl_id, tmpl in DOCX_TEMPLATES.items():
        price = tmpl.get("price", 0)
        label = tmpl["title"]
        if price > 0:
            label += f" ({price:,} ₸)"
        else:
            label += " (бесплатно)"
        buttons.append([InlineKeyboardButton(
            text=label,
            callback_data=f"dxgen_{tmpl_id}",
        )])

    await message.answer(
        "📝 <b>Генератор юридических документов (.docx)</b>\n\n"
        "Выберите шаблон — бот задаст вопросы и сгенерирует\n"
        "профессиональный документ по законодательству РК.\n\n"
        "⚖️ <i>Рекомендуем проверку юристом перед подписанием.</i>",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
    )


@router.callback_query(F.data.startswith("dxgen_"))
async def start_docgen(callback: CallbackQuery, state: FSMContext) -> None:
    """Начало генерации документа — первый вопрос."""
    template_id = callback.data.removeprefix("dxgen_")

    from src.bot.utils.docx_engine import DOCX_TEMPLATES
    template = DOCX_TEMPLATES.get(template_id)
    if not template:
        await callback.answer("Шаблон не найден", show_alert=True)
        return

    questions = template["questions"]
    await state.update_data(
        docgen_template=template_id,
        docgen_step=0,
        docgen_data={},
    )
    await state.set_state(DocGenStates.answering_questions)

    first_q = questions[0]
    await callback.message.answer(
        f"📋 <b>{template['title']}</b>\n\n"
        f"Шаг 1/{len(questions)}\n\n"
        f"{first_q[1]}"
    )
    await callback.answer()


@router.message(DocGenStates.answering_questions)
async def docgen_answer(message: Message, state: FSMContext, bot: Bot) -> None:
    """Обработка ответов пользователя и переход к следующему вопросу."""
    text = (message.text or "").strip()
    if text.startswith("/"):
        await state.clear()
        return

    data = await state.get_data()
    template_id = data.get("docgen_template", "")
    step = data.get("docgen_step", 0)
    doc_data = data.get("docgen_data", {})

    from src.bot.utils.docx_engine import DOCX_TEMPLATES
    template = DOCX_TEMPLATES.get(template_id)
    if not template:
        await state.clear()
        return

    questions = template["questions"]

    # Обработка дефолтных значений
    field = questions[step][0]
    if text == "-":
        defaults = {
            "city": "Алматы",
            "purpose": "обсуждение возможного сотрудничества",
            "duration_months": "24",
            "scope": "представление интересов по всем вопросам",
            "valid_until": "12 месяцев с даты подписания",
            "amount": "по согласованию сторон",
            "deadline_days": "15",
            "salary": "по согласованию",
            "start_date": "с даты подписания",
            "deadline": "30 календарных дней",
        }
        text = defaults.get(field, text)

    doc_data[field] = text
    step += 1

    if step < len(questions):
        # Следующий вопрос
        next_q = questions[step]
        await state.update_data(docgen_step=step, docgen_data=doc_data)
        await message.answer(f"Шаг {step + 1}/{len(questions)}\n\n{next_q[1]}")
    else:
        # Все данные собраны — генерируем
        await state.clear()
        await message.answer("⏳ Генерирую документ...")

        try:
            from src.bot.utils.docx_engine import generate_document_docx

            filepath = await generate_document_docx(
                template_id=template_id,
                data=doc_data,
                user_id=message.from_user.id,
            )

            if filepath and os.path.exists(filepath):
                doc_file = FSInputFile(filepath)
                ext = Path(filepath).suffix
                await message.answer_document(
                    doc_file,
                    caption=(
                        f"📋 <b>{template['title']}</b>\n\n"
                        f"✅ Документ сгенерирован ({ext})\n\n"
                        f"⚖️ <i>Рекомендуем проверку юристом перед подписанием.</i>"
                    ),
                )

                # Карма
                try:
                    from src.bot.utils.karma import add_karma
                    add_karma(message.from_user.id, 10, "docgen")
                except Exception:
                    pass

                # Сохраняем в vault
                try:
                    from src.bot.utils.docx_engine import encrypt_and_store
                    with open(filepath, "rb") as f:
                        await encrypt_and_store(
                            f.read(), Path(filepath).name,
                            message.from_user.id,
                            {"template": template_id},
                        )
                except Exception:
                    pass
            else:
                await message.answer("❌ Ошибка генерации. Попробуйте позже.")
        except Exception as e:
            logger.error("DocGen error: %s", e)
            await message.answer(f"❌ Ошибка: {e}")


# ═══════════════════════════════════════════════════════════════════════════
#  C3: /mytasks — задачи клиента (Legal Task Tracker)
# ═══════════════════════════════════════════════════════════════════════════


@router.message(Command("mytasks"))
async def cmd_mytasks(message: Message) -> None:
    """Показывает задачи пользователя."""
    user_id = message.from_user.id

    from src.bot.utils.ticket_manager import get_open_tickets, format_ticket_list

    # Все задачи пользователя
    all_tickets = get_open_tickets()
    user_tickets = [t for t in all_tickets if t.get("user_id") == user_id]

    if not user_tickets:
        await message.answer(
            "📋 <b>Ваши задачи</b>\n\n"
            "У вас пока нет активных задач.\n\n"
            "⚖️ <i>Задачи создаются автоматически при обращении за консультацией.</i>"
        )
        return

    text = "📋 <b>Ваши задачи</b>\n\n"
    for t in user_tickets:
        status_map = {
            "new": "🆕 Новая",
            "in_progress": "🔄 В работе",
            "review": "👀 На проверке",
            "done": "✅ Готово",
        }
        status = status_map.get(t["status"], t["status"])
        text += (
            f"• <b>{t['title']}</b>\n"
            f"  {status} | 📅 {t.get('deadline_display', '—')}\n\n"
        )

    await message.answer(text)


# ═══════════════════════════════════════════════════════════════════════════
#  C6: Invoice generation (автоматическое выставление счёта)
# ═══════════════════════════════════════════════════════════════════════════


@router.callback_query(F.data.startswith("invoice_"))
async def send_invoice_for_doc(callback: CallbackQuery, bot: Bot) -> None:
    """Выставляет инвойс за платный документ через Telegram Payments."""
    template_id = callback.data.removeprefix("invoice_")

    from src.bot.utils.docx_engine import DOCX_TEMPLATES
    template = DOCX_TEMPLATES.get(template_id)
    if not template:
        await callback.answer("Шаблон не найден", show_alert=True)
        return

    price = template.get("price", 0)
    if price <= 0:
        await callback.answer("Этот документ бесплатный!", show_alert=True)
        return

    if not settings.PAYMENT_PROVIDER_TOKEN:
        await callback.message.answer(
            "💳 Онлайн-оплата временно недоступна.\n"
            "Свяжитесь с нами для оплаты:",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="📞 Связаться", url="https://t.me/SOLISlegal")],
            ]),
        )
        await callback.answer()
        return

    try:
        await bot.send_invoice(
            chat_id=callback.from_user.id,
            title=template["title"],
            description=f"Генерация документа: {template['title']}",
            payload=f"docgen_{template_id}",
            provider_token=settings.PAYMENT_PROVIDER_TOKEN,
            currency="KZT",
            prices=[{"label": template["title"], "amount": price * 100}],
            start_parameter=f"doc_{template_id}",
        )
    except Exception as e:
        logger.error("Invoice send failed: %s", e)
        await callback.message.answer(f"❌ Ошибка выставления счёта: {e}")

    await callback.answer()
