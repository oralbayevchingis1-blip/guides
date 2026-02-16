"""GENNADY TECH_DIRECTOR_V4.2 — Stage 9: Subscription Callback Sync.

Обработчик проверки подписки на канал.
Stage 9: После подписки проверяем наличие email в CRM —
если нет, запускаем LeadForm.
"""

import asyncio
import logging

from aiogram import Bot, F, Router
from aiogram.fsm.context import FSMContext
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup

from src.bot.keyboards.inline import categories_keyboard, guides_menu_keyboard, subscription_keyboard
from src.bot.utils.cache import TTLCache
from src.bot.utils.google_sheets import GoogleSheetsClient
from src.bot.utils.subscription_check import check_subscription
from src.constants import get_text
from src.database.crud import get_lead_by_user_id, track

router = Router()
logger = logging.getLogger(__name__)


@router.callback_query(F.data == "check_subscription")
async def check_subscription_callback(
    callback: CallbackQuery,
    bot: Bot,
    state: FSMContext,
    google: GoogleSheetsClient,
    cache: TTLCache,
) -> None:
    """Повторная проверка подписки по нажатию кнопки.

    Stage 9: после подтверждения подписки проверяем наличие лида в CRM.
    Если лида нет — запускаем сбор email через LeadForm.
    """
    user_id = callback.from_user.id
    is_subscribed = await check_subscription(user_id, bot)

    texts = await cache.get_or_fetch("texts", google.get_bot_texts)

    if not is_subscribed:
        logger.info("Пользователь %s не подписан", user_id)
        fail_text = get_text(texts, "subscription_fail")
        if not fail_text or fail_text == "subscription_fail":
            fail_text = (
                "Подписка пока не найдена. Нажмите «Подписаться на канал» "
                "выше, подпишитесь и вернитесь сюда — я проверю ещё раз."
            )
        await callback.answer(fail_text, show_alert=True)
        return

    # Подписка подтверждена
    await callback.answer("✅ Подписка подтверждена!")
    asyncio.create_task(track(user_id, "sub_confirmed"))
    logger.info("Пользователь %s подтвердил подписку", user_id)

    # Stage 9: Барьер 2 — проверяем наличие email в CRM
    try:
        existing_lead = await get_lead_by_user_id(user_id)
    except Exception:
        existing_lead = None

    if not existing_lead:
        # Лида нет — запускаем сбор данных
        from src.bot.handlers.lead_form import LeadForm

        ask_email_text = get_text(texts, "ask_email")
        if not ask_email_text or ask_email_text == "ask_email":
            ask_email_text = (
                "✅ <b>Подписка подтверждена!</b>\n\n"
                "Укажите email — на него придёт:\n"
                "• ссылка на гайд (чтобы не потерять)\n"
                "• уведомления о новых материалах по вашей теме\n\n"
                "Спама не будет, отписаться — 1 клик.\n\n"
                "💡 Например: <code>name@company.kz</code>"
            )
        if "@" not in ask_email_text:
            ask_email_text += "\n\n💡 Например: <code>name@company.kz</code>"
        await callback.message.answer(ask_email_text)
        await state.set_state(LeadForm.waiting_for_email)
        try:
            await callback.message.delete()
        except Exception:
            pass
        return

    # Оба барьера пройдены — показываем каталог гайдов
    # Если есть pending_guide — показать конкретный гайд вместо каталога
    data = await state.get_data()
    pending = data.get("pending_guide")

    if pending:
        catalog = await cache.get_or_fetch("catalog", google.get_guides_catalog)
        from src.bot.handlers.lead_form import _find_guide
        guide_info = _find_guide(catalog, pending)

        if guide_info:
            guide_id = guide_info.get("id", pending)
            kb = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(
                    text=f"📥 Получить: {guide_info['title']}",
                    callback_data=f"download_{guide_id}",
                )],
                [InlineKeyboardButton(
                    text="📚 Все темы", callback_data="show_categories",
                )],
            ])
            try:
                await callback.message.edit_text(
                    f"✅ <b>Подписка подтверждена!</b>\n\n"
                    f"📚 <b>{guide_info['title']}</b>\n\n"
                    f"{guide_info.get('description', '')}\n\n"
                    "Нажмите кнопку ниже, чтобы получить гайд:",
                    reply_markup=kb,
                )
            except Exception:
                await callback.message.answer(
                    f"✅ Подписка подтверждена!\n\n📚 <b>{guide_info['title']}</b>",
                    reply_markup=kb,
                )
            return

    catalog = await cache.get_or_fetch("catalog", google.get_guides_catalog)
    await callback.message.edit_text(
        get_text(texts, "subscription_success"),
        reply_markup=categories_keyboard(catalog),
    )
