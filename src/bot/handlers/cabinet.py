"""Личный кабинет — профиль, карма, документы, история.

Зашифрованное хранение документов пользователя.
KYC-верификация через Telegram Passport (при настройке).

Команды:
    /profile — личный кабинет
    /karma   — карма и уровень
"""

import logging

from aiogram import Bot, F, Router
from aiogram.filters import Command
from aiogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
)

from src.bot.utils.karma import (
    add_karma,
    get_karma,
    get_karma_leaderboard,
    get_karma_log,
    get_karma_profile,
)
from src.config import settings
from src.database.crud import count_referrals, get_leads_by_user

router = Router()
logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════════════════
#  /profile — Личный кабинет
# ═══════════════════════════════════════════════════════════════════════════


@router.message(Command("profile"))
async def cmd_profile(message: Message, bot: Bot) -> None:
    """Показывает личный кабинет пользователя."""
    user_id = message.from_user.id
    name = message.from_user.full_name or ""
    username = message.from_user.username or ""

    # Карма
    karma_text = get_karma_profile(user_id)

    # Статистика
    ref_count = await count_referrals(user_id)
    leads = await get_leads_by_user(user_id)
    guides_count = len(leads)

    # Часовой пояс
    from src.bot.utils.timezone_manager import get_user_tz, get_user_local_time
    tz = get_user_tz(user_id)
    local_time = get_user_local_time(user_id)

    text = (
        f"👤 <b>Личный кабинет</b>\n\n"
        f"📛 {name}\n"
        f"{'@' + username if username else ''}\n\n"
        f"───────────────\n"
        f"{karma_text}\n"
        f"───────────────\n\n"
        f"📊 <b>Статистика:</b>\n"
        f"  📚 Гайдов скачано: {guides_count}\n"
        f"  🤝 Рефералов: {ref_count}\n"
        f"  🕐 Часовой пояс: {tz}\n"
        f"  🕐 Местное время: {local_time.strftime('%H:%M')}\n"
    )

    buttons = [
        [InlineKeyboardButton(text="⭐ Карма и награды", callback_data="karma_details")],
        [InlineKeyboardButton(text="🤝 Реферальная программа", callback_data="profile_referral")],
        [InlineKeyboardButton(text="🕐 Часовой пояс", callback_data="profile_timezone")],
        [InlineKeyboardButton(text="📝 Генератор документов", callback_data="profile_docs")],
        [InlineKeyboardButton(text="🏪 Премиум услуги", callback_data="profile_shop")],
    ]

    await message.answer(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))


@router.callback_query(F.data == "karma_details")
async def karma_details(callback: CallbackQuery) -> None:
    """Подробности кармы: последние начисления + лидерборд."""
    user_id = callback.from_user.id
    karma_text = get_karma_profile(user_id)

    # Последние начисления
    log = get_karma_log(user_id, limit=5)
    log_text = ""
    if log:
        for entry in reversed(log):
            log_text += f"  +{entry['points']} — {entry['action']}\n"
    else:
        log_text = "  <i>Нет начислений</i>\n"

    # Лидерборд
    top = get_karma_leaderboard(5)
    lb_text = ""
    for item in top:
        medal = ["🥇", "🥈", "🥉", "4️⃣", "5️⃣"][item["rank"] - 1]
        lb_text += f"  {medal} {item['emoji']} {item['karma']} баллов\n"

    if not lb_text:
        lb_text = "  <i>Пока пусто</i>\n"

    text = (
        f"⭐ <b>Карма и достижения</b>\n\n"
        f"{karma_text}\n"
        f"───────────────\n\n"
        f"📜 <b>Последние начисления:</b>\n"
        f"{log_text}\n"
        f"🏆 <b>Топ-5 пользователей:</b>\n"
        f"{lb_text}\n"
        f"───────────────\n"
        f"💡 <b>Как заработать карму:</b>\n"
        f"  📚 Скачать гайд: +10\n"
        f"  🤝 Привести друга: +20\n"
        f"  🤖 AI-консультация: +3\n"
        f"  📝 Сгенерировать документ: +5\n"
        f"  💰 Покупка: +50\n"
    )

    await callback.message.edit_text(
        text,
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="⬅️ Назад", callback_data="back_to_profile")],
            ]
        ),
    )
    await callback.answer()


@router.callback_query(F.data == "back_to_profile")
async def back_to_profile(callback: CallbackQuery, bot: Bot) -> None:
    """Возврат в профиль."""
    # Делегируем в cmd_profile
    await callback.message.delete()
    # Создаём fake message чтобы вызвать cmd_profile
    await callback.answer()
    # Просто предлагаем нажать /profile
    await callback.message.answer("Используйте /profile для возврата в личный кабинет.")


@router.callback_query(F.data == "profile_referral")
async def profile_referral(callback: CallbackQuery) -> None:
    await callback.answer("Используйте /referral", show_alert=True)


@router.callback_query(F.data == "profile_timezone")
async def profile_timezone(callback: CallbackQuery) -> None:
    await callback.answer("Используйте /timezone", show_alert=True)


@router.callback_query(F.data == "profile_docs")
async def profile_docs(callback: CallbackQuery) -> None:
    await callback.answer("Используйте /doc", show_alert=True)


@router.callback_query(F.data == "profile_shop")
async def profile_shop(callback: CallbackQuery) -> None:
    await callback.answer("Используйте /shop", show_alert=True)


# ═══════════════════════════════════════════════════════════════════════════
#  /karma — Быстрая проверка кармы
# ═══════════════════════════════════════════════════════════════════════════


@router.message(Command("karma"))
async def cmd_karma(message: Message) -> None:
    """Показывает карму пользователя."""
    text = get_karma_profile(message.from_user.id)
    await message.answer(text)
