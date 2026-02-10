"""Генерация юридических документов — обработчики.

/doc — выбор шаблона → заполнение данных → генерация PDF → отправка.
"""

import logging
import os

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

from src.bot.utils.pdf_generator import DOCUMENT_TEMPLATES

router = Router()
logger = logging.getLogger(__name__)


class DocGenStates(StatesGroup):
    """FSM для генерации документов."""
    choosing_template = State()
    entering_party1 = State()
    entering_party2 = State()
    entering_city = State()
    entering_purpose = State()
    # Для договора
    entering_service = State()
    entering_client = State()
    entering_company = State()
    entering_amount = State()


# ═══════════════════════════════════════════════════════════════════════════
#  /doc — Каталог шаблонов
# ═══════════════════════════════════════════════════════════════════════════


@router.message(Command("doc"))
async def cmd_doc(message: Message, state: FSMContext) -> None:
    """Показывает доступные шаблоны документов."""
    buttons = []
    for tmpl_id, tmpl in DOCUMENT_TEMPLATES.items():
        buttons.append([InlineKeyboardButton(
            text=tmpl["title"],
            callback_data=f"docgen_{tmpl_id}",
        )])

    # L2: Умные конструкторы (Interactive Wizard)
    buttons.append([InlineKeyboardButton(
        text="🧙 NDA — Умный конструктор",
        callback_data="wizard_nda_wizard",
    )])
    buttons.append([InlineKeyboardButton(
        text="🧙 Трудовой договор — Конструктор",
        callback_data="wizard_employment_wizard",
    )])

    await message.answer(
        "📝 <b>Генерация юридических документов</b>\n\n"
        "Выберите шаблон — бот соберёт данные и сгенерирует "
        "документ за несколько секунд.\n\n"
        "🧙 <b>Умные конструкторы</b> — AI задаст уточняющие вопросы "
        "и создаст кастомный документ.\n\n"
        "⚖️ <i>Документы носят ознакомительный характер. "
        "Рекомендуем проверку юристом.</i>",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
    )
    await state.set_state(DocGenStates.choosing_template)


# ═══════════════════════════════════════════════════════════════════════════
#  NDA Flow
# ═══════════════════════════════════════════════════════════════════════════


@router.callback_query(F.data == "docgen_nda")
async def start_nda(callback: CallbackQuery, state: FSMContext) -> None:
    """Начало генерации NDA."""
    await state.update_data(template="nda")
    await callback.message.answer(
        "📄 <b>Генерация NDA</b>\n\n"
        "Укажите название <b>Раскрывающей стороны</b> (ваша компания):"
    )
    await state.set_state(DocGenStates.entering_party1)
    await callback.answer()


@router.message(DocGenStates.entering_party1)
async def enter_party1(message: Message, state: FSMContext) -> None:
    await state.update_data(party1=message.text.strip())
    await message.answer("Укажите название <b>Получающей стороны</b> (контрагент):")
    await state.set_state(DocGenStates.entering_party2)


@router.message(DocGenStates.entering_party2)
async def enter_party2(message: Message, state: FSMContext) -> None:
    await state.update_data(party2=message.text.strip())
    await message.answer(
        "Город подписания (или отправьте <code>-</code> для Алматы):"
    )
    await state.set_state(DocGenStates.entering_city)


@router.message(DocGenStates.entering_city)
async def enter_city(message: Message, state: FSMContext) -> None:
    city = message.text.strip()
    if city == "-":
        city = "Алматы"
    await state.update_data(city=city)
    await message.answer(
        "Цель соглашения (например: «обсуждение сотрудничества»).\n"
        "Отправьте <code>-</code> для стандартной формулировки:"
    )
    await state.set_state(DocGenStates.entering_purpose)


@router.message(DocGenStates.entering_purpose)
async def enter_purpose_and_generate(message: Message, state: FSMContext, bot: Bot) -> None:
    """Финальный шаг — генерация NDA."""
    purpose = message.text.strip()
    if purpose == "-":
        purpose = "обсуждение возможного сотрудничества"
    await state.update_data(purpose=purpose)

    data = await state.get_data()
    await state.clear()

    await message.answer("⏳ Генерирую документ...")

    try:
        from src.bot.utils.pdf_generator import generate_nda_pdf

        filepath = await generate_nda_pdf(
            party1=data["party1"],
            party2=data["party2"],
            city=data.get("city", "Алматы"),
            purpose=purpose,
            user_name=message.from_user.full_name or "",
        )

        if filepath and os.path.exists(filepath):
            doc = FSInputFile(filepath)
            await message.answer_document(
                doc,
                caption=(
                    f"📄 <b>NDA — Соглашение о неразглашении</b>\n\n"
                    f"Стороны: {data['party1']} ↔ {data['party2']}\n"
                    f"Город: {data.get('city', 'Алматы')}\n\n"
                    f"⚖️ <i>Рекомендуем проверку юристом перед подписанием.</i>"
                ),
            )

            # Карма за генерацию
            try:
                from src.bot.utils.karma import add_karma
                add_karma(message.from_user.id, 5, "doc_generated")
            except Exception:
                pass
        else:
            await message.answer("❌ Ошибка генерации. Попробуйте позже.")
    except Exception as e:
        logger.error("NDA generation error: %s", e)
        await message.answer(f"❌ Ошибка: {e}")


# ═══════════════════════════════════════════════════════════════════════════
#  Contract Flow
# ═══════════════════════════════════════════════════════════════════════════


@router.callback_query(F.data == "docgen_contract")
async def start_contract(callback: CallbackQuery, state: FSMContext) -> None:
    await state.update_data(template="contract")
    await callback.message.answer(
        "📋 <b>Генерация договора</b>\n\n"
        "Опишите услугу (например: «Регистрация ТОО в МФЦА»):"
    )
    await state.set_state(DocGenStates.entering_service)
    await callback.answer()


@router.message(DocGenStates.entering_service)
async def enter_service(message: Message, state: FSMContext) -> None:
    await state.update_data(service_name=message.text.strip())
    await message.answer("ФИО контактного лица заказчика:")
    await state.set_state(DocGenStates.entering_client)


@router.message(DocGenStates.entering_client)
async def enter_client(message: Message, state: FSMContext) -> None:
    await state.update_data(client_name=message.text.strip())
    await message.answer("Название компании заказчика (или <code>-</code> если физ.лицо):")
    await state.set_state(DocGenStates.entering_company)


@router.message(DocGenStates.entering_company)
async def enter_company(message: Message, state: FSMContext) -> None:
    company = message.text.strip()
    if company == "-":
        company = ""
    await state.update_data(client_company=company)
    await message.answer("Сумма договора (или <code>-</code> для «по согласованию»):")
    await state.set_state(DocGenStates.entering_amount)


@router.message(DocGenStates.entering_amount)
async def enter_amount_and_generate(message: Message, state: FSMContext, bot: Bot) -> None:
    amount = message.text.strip()
    if amount == "-":
        amount = ""

    data = await state.get_data()
    await state.clear()

    await message.answer("⏳ Генерирую договор...")

    try:
        from src.bot.utils.pdf_generator import generate_contract_pdf

        filepath = await generate_contract_pdf(
            service_name=data["service_name"],
            client_name=data["client_name"],
            client_company=data.get("client_company", ""),
            amount=amount,
        )

        if filepath and os.path.exists(filepath):
            doc = FSInputFile(filepath)
            await message.answer_document(
                doc,
                caption=(
                    f"📋 <b>Договор оказания юридических услуг</b>\n\n"
                    f"Услуга: {data['service_name']}\n"
                    f"Заказчик: {data['client_name']}\n\n"
                    f"⚖️ <i>Рекомендуем проверку юристом.</i>"
                ),
            )
        else:
            await message.answer("❌ Ошибка генерации.")
    except Exception as e:
        logger.error("Contract generation error: %s", e)
        await message.answer(f"❌ Ошибка: {e}")
