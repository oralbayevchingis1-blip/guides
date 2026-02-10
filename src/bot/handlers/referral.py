"""Реферальная программа с системой Milestones.

Уровни достижений:
  1 друг  → 🤝 Приветственный бонус
  3 друга → ⭐ Золотой гайд (эксклюзивный материал)
  5 друзей → 💎 Приоритетная поддержка
  10 друзей → 🏆 Бесплатная 15-минутная консультация
  25 друзей → 👑 VIP-партнёр SOLIS

Команда: /referral — получить ссылку и увидеть прогресс.
"""

import logging

from aiogram import Bot, F, Router
from aiogram.filters import Command
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message

from src.bot.utils.growth_engine import (
    REFERRAL_MILESTONES,
    check_referral_milestone,
    get_next_milestone,
    referral_progress_text,
)
from src.config import settings
from src.database.crud import count_referrals, save_referral

router = Router()
logger = logging.getLogger(__name__)


@router.message(Command("referral"))
async def cmd_referral(message: Message, bot: Bot) -> None:
    """Показывает реферальную ссылку, прогресс и milestones."""
    if message.from_user is None:
        return

    user_id = message.from_user.id
    bot_info = await bot.get_me()
    bot_username = bot_info.username

    ref_link = f"https://t.me/{bot_username}?start=ref_{user_id}"
    ref_count = await count_referrals(user_id)

    # Прогресс по milestones
    progress = referral_progress_text(ref_count)
    next_ms = get_next_milestone(ref_count)

    text = (
        "🤝 <b>Реферальная программа SOLIS Partners</b>\n\n"
        f"Ваша ссылка:\n<code>{ref_link}</code>\n\n"
        f"👥 Приведено друзей: <b>{ref_count}</b>\n\n"
        "───────────────\n"
        "🏆 <b>Достижения:</b>\n"
        f"{progress}\n"
        "───────────────\n"
    )

    if next_ms:
        remaining = next_ms["count"] - ref_count
        text += (
            f"\n💡 До следующей награды: <b>{remaining}</b> "
            f"{'человек' if remaining > 1 else 'человека'}\n"
            f"   {next_ms['emoji']} {next_ms['text']}\n"
        )

    # Кнопки
    share_text = (
        "Рекомендую бесплатные юридические гайды от SOLIS Partners! "
        "Полезно для IT-бизнеса, стартапов и корпоративного права 🇰🇿"
    )
    share_url = f"https://t.me/share/url?url={ref_link}&text={share_text}"

    buttons = [
        [InlineKeyboardButton(text="📤 Поделиться ссылкой", url=share_url)],
        [InlineKeyboardButton(text="📚 Посмотреть гайды", callback_data="show_all_guides")],
    ]

    # Кнопка получения награды если достигнут milestone
    current_ms = check_referral_milestone(ref_count)
    if current_ms:
        buttons.insert(0, [InlineKeyboardButton(
            text=f"🎁 Забрать награду: {current_ms['emoji']} {current_ms['reward']}",
            callback_data=f"claim_reward_{current_ms['reward']}",
        )])

    await message.answer(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))


@router.callback_query(F.data == "referral_share")
async def referral_share(callback: CallbackQuery, bot: Bot) -> None:
    """Обработка кнопки 'Поделиться ботом' из after_guide_keyboard."""
    if callback.from_user is None:
        return

    user_id = callback.from_user.id
    bot_info = await bot.get_me()
    ref_link = f"https://t.me/{bot_info.username}?start=ref_{user_id}"
    ref_count = await count_referrals(user_id)
    next_ms = get_next_milestone(ref_count)

    text = (
        "🤝 <b>Поделитесь с друзьями!</b>\n\n"
        f"Ваша ссылка:\n<code>{ref_link}</code>\n\n"
        f"👥 Приведено: <b>{ref_count}</b>\n"
    )
    if next_ms:
        remaining = next_ms["count"] - ref_count
        text += f"⏳ До {next_ms['emoji']} награды: <b>{remaining}</b>\n"

    share_text = "Бесплатные юридические гайды от SOLIS Partners 🇰🇿"
    share_url = f"https://t.me/share/url?url={ref_link}&text={share_text}"

    await callback.message.answer(
        text,
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="📤 Поделиться", url=share_url)],
            ]
        ),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("claim_reward_"))
async def claim_reward(callback: CallbackQuery) -> None:
    """Обработка получения награды за milestone."""
    reward_id = callback.data.removeprefix("claim_reward_")
    user_id = callback.from_user.id
    ref_count = await count_referrals(user_id)

    # Находим milestone
    milestone = None
    for ms in REFERRAL_MILESTONES:
        if ms["reward"] == reward_id:
            milestone = ms
            break

    if not milestone:
        await callback.answer("Награда не найдена", show_alert=True)
        return

    if ref_count < milestone["count"]:
        remaining = milestone["count"] - ref_count
        await callback.answer(
            f"Нужно ещё {remaining} рефералов! 🤝",
            show_alert=True,
        )
        return

    # Выдаём награду
    reward_messages = {
        "first_friend": "Спасибо за первую рекомендацию! 🤝",
        "gold_guide": (
            "⭐ <b>Золотой гайд разблокирован!</b>\n\n"
            "Эксклюзивный материал по M&A-сделкам в Казахстане.\n"
            "Наш юрист подготовит его для вас в ближайшее время.\n\n"
            "Мы свяжемся с вами через @SOLISlegal"
        ),
        "priority_support": (
            "💎 <b>Приоритетная поддержка активирована!</b>\n\n"
            "Ваши вопросы через /consult теперь обрабатываются первыми."
        ),
        "free_consult": (
            "🏆 <b>Бесплатная консультация разблокирована!</b>\n\n"
            "15-минутная консультация с юристом SOLIS Partners.\n"
            "Для записи напишите: @SOLISlegal"
        ),
        "vip_partner": (
            "👑 <b>VIP-партнёр SOLIS Partners!</b>\n\n"
            "Персональный менеджер и скидка 20% на все услуги.\n"
            "Мы свяжемся с вами лично. Спасибо за доверие!"
        ),
    }

    text = reward_messages.get(reward_id, f"🎁 Награда: {reward_id}")
    await callback.message.answer(text)
    await callback.answer("🎉 Награда получена!")

    # Уведомляем админа
    try:
        name = callback.from_user.full_name or ""
        username = callback.from_user.username or ""
        await callback.message.bot.send_message(
            chat_id=settings.ADMIN_ID,
            text=(
                f"🏆 <b>Реферальная награда!</b>\n\n"
                f"👤 {name} (@{username})\n"
                f"🎁 Награда: {milestone['emoji']} {milestone['reward']}\n"
                f"👥 Рефералов: {ref_count}\n"
                f"🆔 <code>{user_id}</code>"
            ),
        )
    except Exception as e:
        logger.warning("Reward admin notification failed: %s", e)


async def notify_referrer(
    bot: Bot,
    referrer_id: int,
    new_user_name: str,
) -> None:
    """Уведомляет реферера о новом пользователе + прогресс до milestone."""
    try:
        ref_count = await count_referrals(referrer_id)
        next_ms = get_next_milestone(ref_count)
        milestone = check_referral_milestone(ref_count)

        text = (
            f"🎉 По вашей ссылке пришёл: <b>{new_user_name}</b>\n\n"
            f"👥 Всего рефералов: <b>{ref_count}</b>\n"
        )

        # Если достигнут milestone — супер-уведомление
        if milestone:
            text += (
                f"\n{milestone['emoji']} <b>ДОСТИЖЕНИЕ!</b>\n"
                f"{milestone['text']}\n"
            )
        elif next_ms:
            remaining = next_ms["count"] - ref_count
            text += (
                f"\n⏳ До {next_ms['emoji']} награды: "
                f"<b>{remaining}</b> {'человек' if remaining > 1 else 'человек'}"
            )

        buttons = []
        if milestone:
            buttons.append([InlineKeyboardButton(
                text=f"🎁 Забрать награду",
                callback_data=f"claim_reward_{milestone['reward']}",
            )])

        keyboard = InlineKeyboardMarkup(inline_keyboard=buttons) if buttons else None
        await bot.send_message(chat_id=referrer_id, text=text, reply_markup=keyboard)

    except Exception as e:
        logger.warning("Referrer notification failed %s: %s", referrer_id, e)
