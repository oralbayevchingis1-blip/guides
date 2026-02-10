"""Проактивный дайджест: мониторинг новостей, ежедневные предложения, напоминания по лидам.

Расписание (UTC+5 Алматы):
    09:00 — утренний дайджест (новости + идеи контента)
    18:00 — вечерний отчёт (лиды за день + напоминания)
"""

import asyncio
import logging
from datetime import datetime, timezone, timedelta

from aiogram import Bot, F, Router
from aiogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
)

from src.bot.utils.cache import TTLCache
from src.bot.utils.google_sheets import GoogleSheetsClient
from src.config import settings

router = Router()
logger = logging.getLogger(__name__)

ALMATY_TZ = timezone(timedelta(hours=5))


def register_scheduled_jobs(
    scheduler,
    *,
    bot: Bot,
    google: GoogleSheetsClient,
    cache: TTLCache,
) -> None:
    """Регистрирует ежедневные задачи в APScheduler."""

    async def _morning_digest():
        await send_morning_digest(bot=bot, google=google, cache=cache)

    async def _evening_report():
        await send_evening_report(bot=bot, google=google, cache=cache)

    # Утренний дайджест — 09:00 Алматы (04:00 UTC)
    scheduler.add_job(
        _morning_digest,
        trigger="cron",
        hour=4, minute=0,  # UTC
        id="morning_digest",
        replace_existing=True,
        misfire_grace_time=3600,
    )

    # Вечерний отчёт — 18:00 Алматы (13:00 UTC)
    scheduler.add_job(
        _evening_report,
        trigger="cron",
        hour=13, minute=0,  # UTC
        id="evening_report",
        replace_existing=True,
        misfire_grace_time=3600,
    )

    logger.info("Запланированы: утренний дайджест (09:00) и вечерний отчёт (18:00) Алматы")


# ═══════════════════════════════════════════════════════════════════════
#  УТРЕННИЙ ДАЙДЖЕСТ
# ═══════════════════════════════════════════════════════════════════════


async def send_morning_digest(
    *,
    bot: Bot,
    google: GoogleSheetsClient,
    cache: TTLCache,
) -> None:
    """Утренний дайджест: новости + идеи контента для админа."""
    try:
        from src.bot.utils.news_parser import fetch_all_news
        from src.bot.utils.ai_client import ask_digest

        # 1. Парсим новости
        news_items = await fetch_all_news()

        # 2. Загружаем контекст компании
        data_room = await google.get_data_room()
        data_room_text = "\n".join(
            f"[{item.get('category', '')}] {item.get('title', '')}: {item.get('content', '')[:200]}"
            for item in data_room[:20]
        )

        # 3. Получаем историю контента (чтобы не повторяться)
        recent_articles = await google.get_articles_list(limit=10)
        history_text = ", ".join(a.get("title", "") for a in recent_articles)

        # 4. Формируем промпт для AI
        news_text = ""
        for i, item in enumerate(news_items[:10], 1):
            news_text += f"{i}. [{item.get('source', '')}] {item.get('title', '')}\n   {item.get('url', '')}\n"

        if not news_text:
            news_text = "(Новых релевантных новостей не найдено)"

        context = (
            f"ДАННЫЕ О КОМПАНИИ:\n{data_room_text or '(дата-рум пуст)'}\n\n"
            f"НЕДАВНО ОПУБЛИКОВАНО (не повторять):\n{history_text or '(пока нет)'}\n\n"
            f"СВЕЖИЕ НОВОСТИ:\n{news_text}"
        )

        ai_response = await ask_digest(
            prompt=(
                "Предложи 1-3 идеи контента на сегодня. Для каждой:\n"
                "1. Заголовок статьи/поста\n"
                "2. Тип: статья на сайт / пост в канал / и то и другое\n"
                "3. Почему это актуально (1 предложение)\n"
                "4. Как связать с услугами SOLIS Partners (1 предложение)\n"
                "Формат: пронумерованный список, кратко."
            ),
            context=context,
            max_tokens=2048,
        )

        # 5. Сохраняем новости в Sheets
        for item in news_items[:10]:
            asyncio.create_task(google.append_news(
                source=item.get("source", ""),
                title=item.get("title", ""),
                url=item.get("url", ""),
                summary=item.get("summary", ""),
            ))

        # 6. Отправляем админу
        now = datetime.now(ALMATY_TZ)
        header = f"🌅 *Утренний дайджест — {now.strftime('%d.%m.%Y')}*\n\n"

        news_count = len(news_items)
        header += f"📰 Найдено новостей: {news_count}\n\n"

        message = header + ai_response

        # Ограничиваем длину сообщения
        if len(message) > 4000:
            message = message[:4000] + "..."

        keyboard = InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(
                    text="📝 Опубликовать идею", callback_data="digest_publish"
                )],
                [InlineKeyboardButton(
                    text="📢 Пост в канал", callback_data="digest_channel"
                )],
                [InlineKeyboardButton(
                    text="💡 Ещё идеи", callback_data="digest_more"
                )],
                [InlineKeyboardButton(
                    text="✅ Принято", callback_data="digest_ack"
                )],
            ]
        )

        try:
            await bot.send_message(
                chat_id=settings.ADMIN_ID,
                text=message,
                reply_markup=keyboard,
            )
        except Exception:
            # Markdown-ошибка — отправляем без форматирования
            await bot.send_message(
                chat_id=settings.ADMIN_ID,
                text=message,
                reply_markup=keyboard,
                parse_mode=None,
            )
        logger.info("Утренний дайджест отправлен")

    except Exception as e:
        logger.error("Ошибка утреннего дайджеста: %s", e)
        try:
            await bot.send_message(
                chat_id=settings.ADMIN_ID,
                text=f"⚠️ Не удалось сформировать утренний дайджест: {e}",
            )
        except Exception:
            pass


# ═══════════════════════════════════════════════════════════════════════
#  ВЕЧЕРНИЙ ОТЧЁТ (ЛИДЫ + НАПОМИНАНИЯ)
# ═══════════════════════════════════════════════════════════════════════


async def send_evening_report(
    *,
    bot: Bot,
    google: GoogleSheetsClient,
    cache: TTLCache,
) -> None:
    """Вечерний отчёт: лиды за день + напоминание связаться."""
    try:
        today = datetime.now(ALMATY_TZ).strftime("%d.%m.%Y")
        leads = await google.get_recent_leads(limit=50)

        # Фильтруем сегодняшние лиды
        today_leads = [l for l in leads if l.get("timestamp", "").startswith(today)]

        if not today_leads:
            await bot.send_message(
                chat_id=settings.ADMIN_ID,
                text=f"📊 *Вечерний отчёт — {today}*\n\nНовых лидов за сегодня: 0\nЗавтра будет больше! 💪",
            )
            return

        # Формируем сообщение
        text = f"📊 *Вечерний отчёт — {today}*\n\n"
        text += f"🔥 Новых лидов сегодня: *{len(today_leads)}*\n\n"

        buttons = []
        for i, lead in enumerate(today_leads[:5], 1):
            name = lead.get("name", "—")
            email = lead.get("email", "—")
            guide = lead.get("guide", "—")
            username = lead.get("username", "")
            contacted = lead.get("contacted", "")

            status = "✅" if contacted else "⚠️"
            text += f"{status} *{name}* ({email})\n"
            text += f"   📄 {guide}\n"
            if username:
                text += f"   💬 @{username}\n"
            text += "\n"

            if not contacted and username:
                buttons.append([InlineKeyboardButton(
                    text=f"💬 Написать {name}",
                    url=f"https://t.me/{username}",
                )])

        if len(today_leads) > 5:
            text += f"... и ещё {len(today_leads) - 5} лидов\n"

        not_contacted = [l for l in today_leads if not l.get("contacted")]
        if not_contacted:
            text += f"\n⚠️ *Не обработано: {len(not_contacted)}* — свяжитесь с ними!"

        buttons.append([InlineKeyboardButton(
            text="📊 Открыть CRM",
            url=f"https://docs.google.com/spreadsheets/d/{settings.GOOGLE_SPREADSHEET_ID}/edit",
        )])

        kb = InlineKeyboardMarkup(inline_keyboard=buttons) if buttons else None
        try:
            await bot.send_message(
                chat_id=settings.ADMIN_ID,
                text=text,
                reply_markup=kb,
            )
        except Exception:
            await bot.send_message(
                chat_id=settings.ADMIN_ID,
                text=text,
                reply_markup=kb,
                parse_mode=None,
            )
        logger.info("Вечерний отчёт отправлен: %d лидов", len(today_leads))

    except Exception as e:
        logger.error("Ошибка вечернего отчёта: %s", e)


# ═══════════════════════════════════════════════════════════════════════
#  ОБРАБОТКА КНОПОК ДАЙДЖЕСТА
# ═══════════════════════════════════════════════════════════════════════


@router.callback_query(F.data == "digest_ack")
async def digest_acknowledge(callback: CallbackQuery) -> None:
    if callback.from_user.id != settings.ADMIN_ID:
        return
    await callback.answer("Принято!")
    await callback.message.edit_reply_markup(reply_markup=None)


@router.callback_query(F.data == "digest_publish")
async def digest_publish(callback: CallbackQuery) -> None:
    """Переход к публикации статьи из дайджеста."""
    if callback.from_user.id != settings.ADMIN_ID:
        return
    await callback.answer()
    await callback.message.answer(
        "📝 Скопируйте идею из дайджеста выше и отправьте /publish — "
        "AI автоматически оформит её в статью."
    )


@router.callback_query(F.data == "digest_channel")
async def digest_channel(callback: CallbackQuery) -> None:
    """Быстрый пост в канал из дайджеста."""
    if callback.from_user.id != settings.ADMIN_ID:
        return
    await callback.answer()
    await callback.message.answer(
        "📢 Скопируйте идею и отправьте мне — я сгенерирую пост для канала.\n"
        "Или нажмите /admin -> Маркетинг -> Пост в канал."
    )


@router.callback_query(F.data == "digest_more")
async def digest_more_ideas(
    callback: CallbackQuery,
    google: GoogleSheetsClient,
    cache: TTLCache,
) -> None:
    """Генерирует дополнительные идеи контента."""
    if callback.from_user.id != settings.ADMIN_ID:
        return
    await callback.answer("Генерирую ещё идеи...")

    try:
        from src.bot.utils.ai_client import ask_marketing

        data_room = await google.get_data_room()
        context = "\n".join(
            f"{item.get('title', '')}: {item.get('content', '')[:100]}"
            for item in data_room[:10]
        )

        response = await ask_marketing(
            prompt=(
                "Придумай 3 КРЕАТИВНЫЕ и НЕОБЫЧНЫЕ идеи для контента:\n"
                "- Статья / пост / серия постов\n"
                "Будь конкретным, предложи заголовки и краткое описание."
            ),
            context=context or "(дата-рум пуст)",
            max_tokens=1024,
            temperature=0.9,
        )

        await callback.message.answer(
            f"💡 *Дополнительные идеи:*\n\n{response}",
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[
                    [InlineKeyboardButton(text="📝 Опубликовать", callback_data="digest_publish")],
                    [InlineKeyboardButton(text="✅ Достаточно", callback_data="digest_ack")],
                ]
            ),
        )

    except Exception as e:
        await callback.message.answer(f"❌ Ошибка: {e}")
