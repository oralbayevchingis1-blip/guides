"""Юридический интеллект — обработчики для Legal Tools.

L1: /review — AI DocReview (анализ договоров)
L2: Smart Templates Wizard (улучшенный /doc)
L5: /brainstorm — мульти-агентный консилиум
L6: BIN-check — проверка контрагента по БИН
L7: /tasks — система тикетов для юристов
L10: /remind — ассистент по дедлайнам
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

class LegalToolsStates(StatesGroup):
    waiting_for_document = State()      # L1: ожидание загрузки файла
    doc_review_question = State()       # L1: доп. вопрос по документу
    brainstorm_question = State()       # L5: ожидание вопроса для консилиума
    waiting_for_bin = State()           # L6: ожидание БИН
    ticket_title = State()             # L7: название тикета
    ticket_assignee = State()          # L7: ответственный
    ticket_deadline = State()          # L7: дедлайн
    reminder_text = State()            # L10: текст напоминания
    # L2: Wizard states
    wizard_type = State()
    wizard_party_name = State()
    wizard_party_role = State()
    wizard_jurisdiction = State()
    wizard_special_clauses = State()
    wizard_confirm = State()


# ═══════════════════════════════════════════════════════════════════════════
#  L1: /review — AI DocReview
# ═══════════════════════════════════════════════════════════════════════════


@router.message(Command("review"))
async def cmd_review(message: Message, state: FSMContext) -> None:
    """Запуск AI-анализа юридического документа."""
    await state.set_state(LegalToolsStates.waiting_for_document)
    await message.answer(
        "📄 <b>AI DocReview — Анализ юридических рисков</b>\n\n"
        "Загрузите документ для анализа:\n"
        "• <b>PDF</b> — договоры, соглашения\n"
        "• <b>DOCX</b> — Word-документы\n"
        "• <b>Текст</b> — скопируйте текст договора прямо в чат\n\n"
        "🤖 AI найдёт кабальные условия, скрытые риски и даст рекомендации.\n\n"
        "⚖️ <i>Загрузите файл или отправьте текст 👇</i>",
    )


@router.message(LegalToolsStates.waiting_for_document, F.document)
async def handle_document_upload(
    message: Message, state: FSMContext, bot: Bot
) -> None:
    """Обработка загруженного файла для DocReview."""
    doc = message.document
    filename = doc.file_name or "document"
    ext = Path(filename).suffix.lower()

    if ext not in (".pdf", ".docx", ".doc", ".txt"):
        await message.answer(
            "⚠️ Поддерживаются только PDF, DOCX и TXT файлы.\n"
            "Или просто отправьте текст договора сообщением."
        )
        return

    # Скачиваем файл
    thinking = await message.answer("📥 Скачиваю и анализирую документ...")

    try:
        from src.bot.utils.doc_review import extract_text, analyze_legal_document, TEMP_DIR

        file = await bot.get_file(doc.file_id)
        local_path = str(TEMP_DIR / f"{message.from_user.id}_{filename}")
        await bot.download_file(file.file_path, local_path)

        # Извлекаем текст
        text = await extract_text(local_path)

        # Удаляем временный файл
        try:
            os.unlink(local_path)
        except Exception:
            pass

        if not text or len(text) < 50:
            await thinking.delete()
            await message.answer(
                "❌ Не удалось извлечь текст из документа.\n"
                "Попробуйте скопировать текст и отправить его сообщением."
            )
            return

        await thinking.edit_text(
            f"📄 Извлечено {len(text)} символов.\n🔍 AI анализирует риски..."
        )

        # AI-анализ
        review = await analyze_legal_document(text)

        # Кнопки
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(
                text="❓ Задать вопрос по документу",
                callback_data="docreview_question",
            )],
            [InlineKeyboardButton(
                text="🧠 Консилиум экспертов",
                callback_data="docreview_brainstorm",
            )],
            [InlineKeyboardButton(
                text="📞 Связаться с юристом",
                url="https://t.me/SOLISlegal",
            )],
        ])

        await thinking.delete()

        # Сохраняем текст для доп. вопросов
        await state.update_data(doc_text=text[:10000])

        response = (
            f"📄 <b>AI DocReview — Отчёт</b>\n"
            f"📎 Файл: {filename}\n\n"
            f"{review}"
        )

        try:
            await message.answer(response[:4000], reply_markup=keyboard)
        except Exception:
            await message.answer(response[:4000], reply_markup=keyboard, parse_mode=None)

        # Карма
        try:
            from src.bot.utils.karma import add_karma
            add_karma(message.from_user.id, 10, "doc_review")
        except Exception:
            pass

    except Exception as e:
        logger.error("DocReview error: %s", e)
        try:
            await thinking.delete()
        except Exception:
            pass
        await message.answer(f"❌ Ошибка анализа: {e}")

    await state.clear()


@router.message(LegalToolsStates.waiting_for_document)
async def handle_document_text(
    message: Message, state: FSMContext
) -> None:
    """Обработка текста документа (вместо файла)."""
    text = message.text or ""
    if text.startswith("/"):
        await state.clear()
        return

    if len(text) < 50:
        await message.answer("Текст слишком короткий. Отправьте полный текст договора (мин. 50 символов).")
        return

    thinking = await message.answer("🔍 Анализирую текст договора...")

    try:
        from src.bot.utils.doc_review import analyze_legal_document

        review = await analyze_legal_document(text)

        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(
                text="❓ Задать вопрос",
                callback_data="docreview_question",
            )],
            [InlineKeyboardButton(
                text="📞 Связаться с юристом",
                url="https://t.me/SOLISlegal",
            )],
        ])

        await thinking.delete()
        await state.update_data(doc_text=text[:10000])

        try:
            await message.answer(
                f"📄 <b>AI DocReview — Отчёт</b>\n\n{review}"[:4000],
                reply_markup=keyboard,
            )
        except Exception:
            await message.answer(review[:4000], reply_markup=keyboard, parse_mode=None)

    except Exception as e:
        logger.error("DocReview text error: %s", e)
        try:
            await thinking.delete()
        except Exception:
            pass
        await message.answer(f"❌ Ошибка: {e}")

    await state.clear()


@router.callback_query(F.data == "docreview_question")
async def docreview_ask_question(callback: CallbackQuery, state: FSMContext) -> None:
    """Доп. вопрос по проанализированному документу."""
    await state.set_state(LegalToolsStates.doc_review_question)
    await callback.message.answer("❓ Задайте вопрос по этому документу:")
    await callback.answer()


@router.message(LegalToolsStates.doc_review_question)
async def handle_doc_question(
    message: Message, state: FSMContext
) -> None:
    """Обработка доп. вопроса по документу."""
    question = message.text or ""
    if question.startswith("/"):
        await state.clear()
        return

    data = await state.get_data()
    doc_text = data.get("doc_text", "")

    thinking = await message.answer("🔍 Анализирую...")

    try:
        from src.bot.utils.doc_review import analyze_legal_document
        answer = await analyze_legal_document(doc_text, user_question=question)
        await thinking.delete()
        try:
            await message.answer(answer[:4000])
        except Exception:
            await message.answer(answer[:4000], parse_mode=None)
    except Exception as e:
        await thinking.delete()
        await message.answer(f"❌ Ошибка: {e}")

    await state.clear()


# ═══════════════════════════════════════════════════════════════════════════
#  L2: Smart Templates Wizard
# ═══════════════════════════════════════════════════════════════════════════

WIZARD_TEMPLATES = {
    "nda_wizard": {
        "title": "📝 NDA — Умный конструктор",
        "questions": [
            ("party_name", "Название вашей компании (или ваше ФИО):"),
            ("party_role", "Ваша роль: <b>Раскрывающая</b> или <b>Получающая</b> сторона?\n(отправьте 1 или 2)"),
            ("counterparty", "Название компании контрагента:"),
            ("jurisdiction", "Юрисдикция: 🇰🇿 РК или 🏛️ МФЦА?\n(отправьте <code>РК</code> или <code>МФЦА</code>)"),
            ("special_clauses", "Особые условия (или <code>-</code> для стандартных):\n"
             "• Срок конфиденциальности\n• Исключения из конфиденциальности\n• Штрафные санкции"),
        ],
    },
    "employment_wizard": {
        "title": "👔 Трудовой договор — Конструктор",
        "questions": [
            ("employer", "Название компании-работодателя:"),
            ("employee", "ФИО работника:"),
            ("position", "Должность:"),
            ("salary", "Оклад (в тенге, или <code>-</code> для «по согласованию»):"),
            ("special_clauses", "Дополнительные условия (или <code>-</code>):\n"
             "• Испытательный срок\n• НДА\n• Нон-компетишн\n• Удалённая работа"),
        ],
    },
}


@router.callback_query(F.data.startswith("wizard_"))
async def start_wizard(callback: CallbackQuery, state: FSMContext) -> None:
    """Запуск умного конструктора документа."""
    wizard_id = callback.data.removeprefix("wizard_")
    template = WIZARD_TEMPLATES.get(wizard_id)
    if not template:
        await callback.answer("Шаблон не найден", show_alert=True)
        return

    await state.update_data(
        wizard_id=wizard_id,
        wizard_step=0,
        wizard_data={},
    )

    # Задаём первый вопрос
    first_q = template["questions"][0]
    await callback.message.answer(
        f"🧙 <b>{template['title']}</b>\n\n"
        f"Шаг 1/{len(template['questions'])}\n\n"
        f"{first_q[1]}"
    )
    await state.set_state(LegalToolsStates.wizard_party_name)
    await callback.answer()


@router.message(LegalToolsStates.wizard_party_name)
async def wizard_next_step(
    message: Message, state: FSMContext, bot: Bot
) -> None:
    """Обработка шага конструктора и переход к следующему."""
    text = message.text or ""
    if text.startswith("/"):
        await state.clear()
        return

    data = await state.get_data()
    wizard_id = data.get("wizard_id", "")
    step = data.get("wizard_step", 0)
    wizard_data = data.get("wizard_data", {})

    template = WIZARD_TEMPLATES.get(wizard_id)
    if not template:
        await state.clear()
        return

    questions = template["questions"]

    # Сохраняем ответ
    field_name = questions[step][0]
    wizard_data[field_name] = text.strip()
    step += 1

    if step < len(questions):
        # Следующий вопрос
        next_q = questions[step]
        await state.update_data(wizard_step=step, wizard_data=wizard_data)
        await message.answer(
            f"Шаг {step + 1}/{len(questions)}\n\n{next_q[1]}"
        )
    else:
        # Все данные собраны — генерируем документ через AI
        await state.clear()
        await message.answer("⏳ Генерирую документ на основе ваших данных...")

        try:
            from src.bot.utils.ai_client import get_orchestrator
            ai = get_orchestrator()

            prompt = (
                f"Сгенерируй юридический документ (тип: {wizard_id}).\n\n"
                f"Данные клиента:\n"
            )
            for k, v in wizard_data.items():
                prompt += f"  • {k}: {v}\n"
            prompt += (
                "\nТребования:\n"
                "1. Полноценный юридический документ на русском языке\n"
                "2. Соответствие законодательству РК\n"
                "3. Все необходимые разделы и пункты\n"
                "4. Формат: простой текст (для PDF)\n"
                "5. Длина: полный документ, без сокращений"
            )

            doc_text = await ai.call_with_fallback(
                prompt,
                "Ты — юрист SOLIS Partners. Генерируй полноценные юридические документы.",
                primary="openai", max_tokens=4096, temperature=0.3,
            )

            # Сохраняем в файл
            from src.bot.utils.pdf_generator import OUTPUT_DIR
            out_dir = Path(OUTPUT_DIR)
            out_dir.mkdir(parents=True, exist_ok=True)
            filename = f"{wizard_id}_{message.from_user.id}.txt"
            filepath = out_dir / filename
            filepath.write_text(doc_text, encoding="utf-8")

            doc = FSInputFile(str(filepath))
            await message.answer_document(
                doc,
                caption=(
                    f"📋 <b>{template['title']}</b>\n\n"
                    f"✅ Документ сгенерирован по вашим данным.\n\n"
                    f"⚖️ <i>Рекомендуем проверку юристом перед подписанием.</i>"
                ),
            )

            try:
                from src.bot.utils.karma import add_karma
                add_karma(message.from_user.id, 10, "wizard_doc")
            except Exception:
                pass

        except Exception as e:
            logger.error("Wizard generation error: %s", e)
            await message.answer(f"❌ Ошибка генерации: {e}")


# ═══════════════════════════════════════════════════════════════════════════
#  L5: /brainstorm — мульти-агентный консилиум
# ═══════════════════════════════════════════════════════════════════════════


@router.message(Command("brainstorm"))
async def cmd_brainstorm(message: Message, state: FSMContext) -> None:
    """Запуск мульти-агентного юридического консилиума."""
    await state.set_state(LegalToolsStates.brainstorm_question)
    await message.answer(
        "🧠 <b>Юридический консилиум SOLIS Partners</b>\n\n"
        "Три AI-эксперта обсудят ваш вопрос:\n"
        "• ⚖️ Юрист по МФЦА (английское право)\n"
        "• 💰 Налоговый консультант (НК РК)\n"
        "• 🏛️ Корпоративный стратег\n\n"
        "Опишите ваш вопрос подробно 👇",
    )


@router.callback_query(F.data == "docreview_brainstorm")
async def docreview_to_brainstorm(callback: CallbackQuery, state: FSMContext) -> None:
    """Консилиум по документу из DocReview."""
    await state.set_state(LegalToolsStates.brainstorm_question)
    await callback.message.answer("🧠 Опишите вопрос для экспертного обсуждения:")
    await callback.answer()


@router.message(LegalToolsStates.brainstorm_question)
async def handle_brainstorm(
    message: Message, state: FSMContext, google: GoogleSheetsClient, cache: TTLCache,
) -> None:
    """Обработка вопроса для мульти-агентного обсуждения."""
    question = message.text or ""
    if question.startswith("/"):
        await state.clear()
        return

    if len(question) < 10:
        await message.answer("Опишите вопрос подробнее (минимум 10 символов).")
        return

    thinking = await message.answer("🧠 Три эксперта обсуждают ваш вопрос... (30-60 сек)")

    try:
        from src.bot.utils.multi_agent import multi_agent_brainstorm

        data = await state.get_data()
        context = data.get("doc_text", "")

        result = await multi_agent_brainstorm(question, context=context)

        await thinking.delete()

        # Разбиваем на части если слишком длинный
        if len(result) > 4000:
            parts = [result[i:i+4000] for i in range(0, len(result), 4000)]
            for i, part in enumerate(parts):
                try:
                    await message.answer(part)
                except Exception:
                    await message.answer(part, parse_mode=None)
        else:
            try:
                await message.answer(result)
            except Exception:
                await message.answer(result, parse_mode=None)

        # Карма
        try:
            from src.bot.utils.karma import add_karma
            add_karma(message.from_user.id, 15, "brainstorm")
        except Exception:
            pass

    except Exception as e:
        logger.error("Brainstorm error: %s", e)
        try:
            await thinking.delete()
        except Exception:
            pass
        await message.answer(f"❌ Ошибка: {e}")

    await state.clear()


# ═══════════════════════════════════════════════════════════════════════════
#  L6: BIN Check — проверка контрагента
# ═══════════════════════════════════════════════════════════════════════════


@router.message(Command("bin"))
async def cmd_bin(message: Message, state: FSMContext) -> None:
    """Проверка контрагента по БИН."""
    # Проверяем, есть ли БИН прямо в команде
    parts = (message.text or "").split()
    if len(parts) > 1:
        bin_text = parts[1].strip()
        from src.bot.utils.legal_search import is_valid_bin
        if is_valid_bin(bin_text):
            await _process_bin(message, bin_text)
            return

    await state.set_state(LegalToolsStates.waiting_for_bin)
    await message.answer(
        "🏢 <b>Проверка контрагента по БИН</b>\n\n"
        "Отправьте 12-значный БИН компании.\n\n"
        "Бот найдёт информацию в открытых источниках:\n"
        "• Наименование и статус\n"
        "• Вид деятельности\n"
        "• Дата регистрации\n"
        "• Адрес"
    )


@router.message(LegalToolsStates.waiting_for_bin)
async def handle_bin_input(message: Message, state: FSMContext) -> None:
    """Обработка введённого БИН."""
    text = (message.text or "").strip()
    if text.startswith("/"):
        await state.clear()
        return

    await _process_bin(message, text)
    await state.clear()


async def _process_bin(message: Message, bin_text: str) -> None:
    """Обработка БИН и отправка отчёта."""
    from src.bot.utils.legal_search import is_valid_bin, check_counterparty_by_bin, format_bin_report

    if not is_valid_bin(bin_text):
        await message.answer("⚠️ БИН должен содержать ровно 12 цифр. Попробуйте снова.")
        return

    thinking = await message.answer("🔍 Проверяю контрагента...")

    try:
        data = await check_counterparty_by_bin(bin_text)
        report = format_bin_report(data)

        await thinking.delete()

        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(
                text="📞 Полная проверка контрагента",
                url="https://t.me/SOLISlegal",
            )],
        ])

        try:
            await message.answer(report, reply_markup=keyboard)
        except Exception:
            await message.answer(report, reply_markup=keyboard, parse_mode=None)

    except Exception as e:
        logger.error("BIN check error: %s", e)
        try:
            await thinking.delete()
        except Exception:
            pass
        await message.answer(f"❌ Ошибка проверки: {e}")


# ═══════════════════════════════════════════════════════════════════════════
#  L7: /tasks — система тикетов
# ═══════════════════════════════════════════════════════════════════════════


@router.message(Command("tasks"))
async def cmd_tasks(message: Message) -> None:
    """Показывает открытые задачи (только для админа)."""
    if message.from_user.id != settings.ADMIN_ID:
        return

    from src.bot.utils.ticket_manager import get_open_tickets, get_overdue_tickets, format_ticket_list

    tickets = get_open_tickets()
    overdue = get_overdue_tickets()

    text = format_ticket_list(tickets)
    if overdue:
        text += f"\n\n🔴 <b>Просроченных: {len(overdue)}</b>"

    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Новая задача", callback_data="ticket_create")],
    ])

    await message.answer(text, reply_markup=keyboard)


@router.callback_query(F.data == "ticket_create")
async def start_ticket_creation(callback: CallbackQuery, state: FSMContext) -> None:
    """Создание нового тикета."""
    if callback.from_user.id != settings.ADMIN_ID:
        await callback.answer("Только для администратора", show_alert=True)
        return

    await state.set_state(LegalToolsStates.ticket_title)
    await callback.message.answer("📋 <b>Новая задача</b>\n\nВведите название задачи:")
    await callback.answer()


@router.message(LegalToolsStates.ticket_title)
async def enter_ticket_title(message: Message, state: FSMContext) -> None:
    text = message.text or ""
    if text.startswith("/"):
        await state.clear()
        return
    await state.update_data(ticket_title=text.strip())
    await message.answer("👤 Ответственный (имя юриста или <code>-</code>):")
    await state.set_state(LegalToolsStates.ticket_assignee)


@router.message(LegalToolsStates.ticket_assignee)
async def enter_ticket_assignee(message: Message, state: FSMContext) -> None:
    assignee = (message.text or "").strip()
    if assignee == "-":
        assignee = ""
    await state.update_data(ticket_assignee=assignee)
    await message.answer("📅 Дедлайн через сколько дней? (число или <code>-</code> для 7 дней):")
    await state.set_state(LegalToolsStates.ticket_deadline)


@router.message(LegalToolsStates.ticket_deadline)
async def enter_ticket_deadline(
    message: Message, state: FSMContext, google: GoogleSheetsClient,
) -> None:
    text = (message.text or "").strip()
    try:
        days = int(text) if text != "-" else 7
    except ValueError:
        days = 7

    data = await state.get_data()
    await state.clear()

    from src.bot.utils.ticket_manager import create_ticket, format_ticket

    ticket = await create_ticket(
        title=data.get("ticket_title", "Без названия"),
        assignee=data.get("ticket_assignee", ""),
        deadline_days=days,
        user_id=message.from_user.id,
        google=google,
    )

    await message.answer(
        f"✅ Задача создана!\n\n{format_ticket(ticket)}"
    )


@router.callback_query(F.data.startswith("ticket_status_"))
async def update_ticket(callback: CallbackQuery) -> None:
    """Обновление статуса тикета."""
    if callback.from_user.id != settings.ADMIN_ID:
        return

    parts = callback.data.split("_", 3)
    if len(parts) < 4:
        await callback.answer("Ошибка формата", show_alert=True)
        return

    ticket_id = parts[2]
    new_status = parts[3]

    from src.bot.utils.ticket_manager import update_ticket_status
    ok = update_ticket_status(ticket_id, new_status)
    if ok:
        await callback.answer(f"✅ Тикет {ticket_id} → {new_status}")
    else:
        await callback.answer("Ошибка обновления", show_alert=True)


# ═══════════════════════════════════════════════════════════════════════════
#  L10: /remind — ассистент по дедлайнам
# ═══════════════════════════════════════════════════════════════════════════


@router.message(Command("remind"))
async def cmd_remind(message: Message, state: FSMContext) -> None:
    """Ассистент по дедлайнам — установка напоминания."""
    # Проверяем, есть ли текст прямо в команде
    text = (message.text or "").strip()
    parts = text.split(maxsplit=1)
    if len(parts) > 1:
        from src.bot.utils.ticket_manager import parse_deadline_request
        parsed = parse_deadline_request(parts[1])
        if parsed:
            await _create_reminder(message, parsed)
            return

    await state.set_state(LegalToolsStates.reminder_text)
    await message.answer(
        "⏰ <b>Ассистент по дедлайнам</b>\n\n"
        "Опишите, о чём напомнить. Примеры:\n\n"
        "• <code>Напомни подать отчет в МФЦА через месяц</code>\n"
        "• <code>Напомни 15.03.2026 оплатить налог</code>\n"
        "• <code>Напомни через 7 дней продлить лицензию</code>\n\n"
        "Напишите 👇",
    )


@router.message(LegalToolsStates.reminder_text)
async def handle_reminder_text(message: Message, state: FSMContext) -> None:
    """Обработка текста напоминания."""
    text = (message.text or "").strip()
    if text.startswith("/"):
        await state.clear()
        return

    from src.bot.utils.ticket_manager import parse_deadline_request
    parsed = parse_deadline_request(text)

    if not parsed:
        await message.answer(
            "⚠️ Не удалось распознать дату.\n"
            "Используйте формат: «через N дней» или «DD.MM.YYYY»"
        )
        return

    await _create_reminder(message, parsed)
    await state.clear()


async def _create_reminder(message: Message, parsed: dict) -> None:
    """Создаёт напоминание и отправляет подтверждение."""
    from src.bot.utils.ticket_manager import schedule_reminder
    from src.bot.utils.scheduler import get_scheduler

    scheduler = get_scheduler()
    if not scheduler:
        await message.answer("❌ Планировщик недоступен.")
        return

    reminder = await schedule_reminder(
        scheduler=scheduler,
        bot=message.bot,
        user_id=message.from_user.id,
        task=parsed["task"],
        days=parsed["days"],
    )

    await message.answer(
        f"✅ <b>Напоминание создано!</b>\n\n"
        f"📋 {parsed['task']}\n"
        f"📅 Напомню: {reminder['fire_display']}\n"
        f"⏳ Через {parsed['days']} дн.\n\n"
        f"⚖️ <i>Бот уведомит вас и юристов SOLIS Partners.</i>"
    )
