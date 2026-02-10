"""Feedback & NPS — сбор отзывов после AI-консультации.

Через 2 часа после /consult бот спрашивает:
«Был ли полезен ответ?» (1-5 звёзд).
Если 5/5 — просит оставить отзыв на Google Maps.
Если <3 — уведомляет админа для ручной обработки.

Использование:
    from src.bot.handlers.feedback import schedule_feedback
    schedule_feedback(scheduler, bot, user_id)
"""

import logging
from datetime import datetime, timezone

from aiogram import Bot, F, Router
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup

from src.config import settings

router = Router()
logger = logging.getLogger(__name__)

# Google Maps ссылка для отзыва (настроить)
GOOGLE_MAPS_REVIEW_URL = "https://g.page/r/solispartners/review"
INSTAGRAM_URL = "https://www.instagram.com/solis.partners/"

# Хранилище NPS (user_id -> [scores])
_nps_scores: dict[int, list[int]] = {}
_feedback_texts: dict[int, str] = {}


def _stars(n: int) -> str:
    """Генерирует строку звёзд."""
    return "⭐" * n + "☆" * (5 - n)


def _nps_keyboard() -> InlineKeyboardMarkup:
    """Клавиатура оценки 1-5."""
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text=f"{_stars(i)} {i}", callback_data=f"nps_{i}")
                for i in range(1, 6)
            ],
            [
                InlineKeyboardButton(text="⏭ Пропустить", callback_data="nps_skip"),
            ],
        ]
    )


async def send_nps_request(bot: Bot, user_id: int) -> None:
    """Отправляет запрос NPS-оценки пользователю."""
    try:
        await bot.send_message(
            chat_id=user_id,
            text=(
                "💬 <b>Нам важно ваше мнение!</b>\n\n"
                "Был ли полезен ответ нашего AI-юриста?\n"
                "Оцените от 1 до 5:\n\n"
                "───────────────"
            ),
            reply_markup=_nps_keyboard(),
        )
        logger.info("NPS request sent to user_id=%s", user_id)
    except Exception as e:
        logger.warning("NPS send failed for user_id=%s: %s", user_id, e)


@router.callback_query(F.data.startswith("nps_"))
async def handle_nps(callback: CallbackQuery) -> None:
    """Обработка оценки NPS."""
    user_id = callback.from_user.id

    if callback.data == "nps_skip":
        await callback.message.edit_text(
            "Понимаем! Если захотите оставить отзыв позже — напишите /consult 😊"
        )
        await callback.answer()
        return

    try:
        score = int(callback.data.removeprefix("nps_"))
    except ValueError:
        await callback.answer("Ошибка")
        return

    # Сохраняем оценку
    _nps_scores.setdefault(user_id, []).append(score)
    await callback.answer(f"Спасибо за оценку: {_stars(score)}")

    if score >= 4:
        # Высокая оценка — просим отзыв
        await callback.message.edit_text(
            f"🎉 <b>Спасибо за высокую оценку!</b> {_stars(score)}\n\n"
            "Мы будем признательны, если вы оставите отзыв.\n"
            "Это поможет другим найти качественную юридическую помощь!",
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[
                    [InlineKeyboardButton(
                        text="⭐ Отзыв на Google",
                        url=GOOGLE_MAPS_REVIEW_URL,
                    )],
                    [InlineKeyboardButton(
                        text="📸 Подписаться в Instagram",
                        url=INSTAGRAM_URL,
                    )],
                    [InlineKeyboardButton(
                        text="✅ Спасибо, не сейчас",
                        callback_data="nps_thanks",
                    )],
                ]
            ),
        )
    elif score >= 3:
        # Средняя оценка
        await callback.message.edit_text(
            f"Спасибо за оценку! {_stars(score)}\n\n"
            "Мы работаем над улучшением. Если хотите — опишите, "
            "что можно было бы сделать лучше.\n\n"
            "Или задайте новый вопрос: /consult"
        )
    else:
        # Низкая оценка — уведомляем админа
        await callback.message.edit_text(
            f"Нам очень жаль, что ответ не помог. {_stars(score)}\n\n"
            "Мы передадим ваш вопрос живому юристу.\n"
            "Он свяжется с вами в ближайшее время. 🤝\n\n"
            "Или свяжитесь сами: @SOLISlegal"
        )

        # Уведомление админу
        try:
            username = callback.from_user.username or ""
            name = callback.from_user.full_name or ""
            await callback.message.bot.send_message(
                chat_id=settings.ADMIN_ID,
                text=(
                    f"⚠️ <b>Низкая NPS оценка!</b>\n\n"
                    f"👤 {name} (@{username})\n"
                    f"⭐ Оценка: {score}/5\n"
                    f"🆔 User ID: <code>{user_id}</code>\n\n"
                    "Рекомендуется связаться лично."
                ),
                reply_markup=InlineKeyboardMarkup(
                    inline_keyboard=[
                        [InlineKeyboardButton(
                            text=f"💬 Написать @{username}" if username else "📋 User ID",
                            url=f"https://t.me/{username}" if username else
                                f"https://docs.google.com/spreadsheets/d/{settings.GOOGLE_SPREADSHEET_ID}/edit",
                        )],
                    ]
                ),
            )
        except Exception as e:
            logger.error("NPS admin notification failed: %s", e)


@router.callback_query(F.data == "nps_thanks")
async def nps_thanks(callback: CallbackQuery) -> None:
    await callback.message.edit_text(
        "Спасибо! Если возникнут вопросы — мы всегда на связи. ⚖️\n\n"
        "/consult — AI-юрист\n"
        "@SOLISlegal — живой юрист"
    )
    await callback.answer()


def schedule_feedback(scheduler, bot: Bot, user_id: int, delay_hours: float = 2.0) -> None:
    """Планирует отправку NPS-запроса через N часов."""
    from datetime import timedelta

    run_time = datetime.now(timezone.utc) + timedelta(hours=delay_hours)

    scheduler.add_job(
        send_nps_request,
        trigger="date",
        run_date=run_time,
        args=[bot, user_id],
        id=f"nps_{user_id}_{int(run_time.timestamp())}",
        replace_existing=True,
        misfire_grace_time=3600,
    )
    logger.info("NPS scheduled for user_id=%s in %.1fh", user_id, delay_hours)


# ── NPS Analytics ────────────────────────────────────────────────────────


def get_nps_summary() -> dict:
    """Сводка NPS за всё время."""
    all_scores = []
    for scores in _nps_scores.values():
        all_scores.extend(scores)

    if not all_scores:
        return {"total": 0, "avg": 0, "promoters": 0, "detractors": 0, "nps": 0}

    total = len(all_scores)
    avg = sum(all_scores) / total
    promoters = sum(1 for s in all_scores if s >= 4)
    detractors = sum(1 for s in all_scores if s <= 2)
    nps_score = int((promoters - detractors) / total * 100)

    return {
        "total": total,
        "avg": round(avg, 1),
        "promoters": promoters,
        "detractors": detractors,
        "nps": nps_score,
    }
