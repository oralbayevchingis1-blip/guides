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

    # Auto-FAQ Discovery — ежедневно в 02:00 UTC (07:00 Алматы)
    async def _auto_faq():
        from src.bot.utils.auto_faq import run_auto_faq_discovery
        await run_auto_faq_discovery(google=google, cache=cache, bot=bot)

    scheduler.add_job(
        _auto_faq,
        trigger="cron",
        hour=2, minute=0,  # UTC
        id="auto_faq_daily",
        replace_existing=True,
        misfire_grace_time=3600,
    )

    # Проактивный контент-хантер — каждые 2 часа ищет критические новости
    async def _content_hunter():
        await proactive_content_hunter(bot=bot, google=google, cache=cache)

    scheduler.add_job(
        _content_hunter,
        trigger="interval",
        hours=2,
        id="content_hunter_2h",
        replace_existing=True,
        misfire_grace_time=3600,
    )

    logger.info("Запланированы: утренний дайджест (09:00), вечерний отчёт (18:00), контент-хантер (каждые 2ч) Алматы")


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

        # L9: AI-анализ влияния новостей на бизнес клиентов
        impact_analysis = ""
        if news_items:
            try:
                impact_analysis = await ask_digest(
                    prompt=(
                        "Ты — юрист-аналитик SOLIS Partners. Проанализируй эти новости.\n"
                        "Для каждой важной новости напиши:\n"
                        "📌 <b>Для бизнеса это значит:</b> [конкретное влияние]\n"
                        "✅ <b>Рекомендуем:</b> [что сделать клиентам]\n\n"
                        "Если новость требует обновления документов — укажи каких.\n"
                        "Формат: HTML для Telegram. Кратко, по делу."
                    ),
                    context=f"НОВОСТИ:\n{news_text}",
                    max_tokens=1024,
                )
            except Exception as e:
                logger.warning("News impact analysis failed: %s", e)

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
        header = f"🌅 <b>Утренний дайджест — {now.strftime('%d.%m.%Y')}</b>\n\n"

        news_count = len(news_items)
        header += f"📰 Найдено новостей: {news_count}\n\n"

        message = header + ai_response

        # L9: Добавляем анализ влияния если есть
        if impact_analysis:
            message += (
                "\n\n───────────────\n"
                "🔍 <b>Анализ влияния на клиентов:</b>\n\n"
                + impact_analysis
            )

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
                text=f"📊 <b>Вечерний отчёт — {today}</b>\n\nНовых лидов за сегодня: 0\nЗавтра будет больше! 💪",
            )
            return

        # Формируем сообщение
        text = f"📊 <b>Вечерний отчёт — {today}</b>\n\n"
        text += f"🔥 Новых лидов сегодня: <b>{len(today_leads)}</b>\n\n"

        buttons = []
        for i, lead in enumerate(today_leads[:5], 1):
            name = lead.get("name", "—")
            email = lead.get("email", "—")
            guide = lead.get("guide", "—")
            username = lead.get("username", "")
            contacted = lead.get("contacted", "")

            status = "✅" if contacted else "⚠️"
            text += f"{status} <b>{name}</b> ({email})\n"
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
            text += f"\n⚠️ <b>Не обработано: {len(not_contacted)}</b> — свяжитесь с ними!"

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


# ═══════════════════════════════════════════════════════════════════════
#  ПРОАКТИВНЫЙ КОНТЕНТ-ХАНТЕР (Autonomous Drafting)
# ═══════════════════════════════════════════════════════════════════════


async def proactive_content_hunter(
    *,
    bot: Bot,
    google: GoogleSheetsClient,
    cache: TTLCache,
) -> None:
    """Каждые 2 часа ищет критически важные новости.

    Если обнаружены изменения в законах РК или критические новости —
    автоматически создаёт черновик поста в Content Calendar и отправляет
    админу кнопку «Опубликовать в один клик».
    """
    try:
        from src.bot.utils.news_parser import fetch_all_news
        from src.bot.utils.ai_client import ask_marketing

        news_items = await fetch_all_news()
        if not news_items:
            return

        # Формируем текст для AI-анализа
        news_text = "\n".join(
            f"- [{n.get('source', '')}] {n.get('title', '')}: {n.get('summary', '')[:150]}"
            for n in news_items[:15]
        )

        analysis = await ask_marketing(
            prompt=(
                "Проанализируй следующие новости и определи КРИТИЧЕСКИ ВАЖНЫЕ "
                "для клиентов юридической фирмы в Казахстане.\n\n"
                "Критерии критичности:\n"
                "- Изменения в законодательстве РК (новые законы, поправки)\n"
                "- Решения МФЦА, регулятора, ВС РК\n"
                "- Крупные сделки M&A, IPO в регионе\n"
                "- Кибербезопасность и защита данных\n\n"
                "Для КАЖДОЙ критически важной новости верни JSON-массив:\n"
                '[{"title": "Заголовок поста", "type": "article|channel_post", '
                '"urgency": "high|medium", "summary": "Краткое описание 1-2 предложения", '
                '"source_url": "ссылка"}]\n\n'
                "Если нет критических новостей — верни пустой массив: []\n\n"
                f"НОВОСТИ:\n{news_text}"
            ),
            max_tokens=1024,
            temperature=0.3,
        )

        # Парсим JSON из ответа AI
        import json as _json
        import re

        json_match = re.search(r'\[.*\]', analysis, re.DOTALL)
        if not json_match:
            return

        try:
            critical_items = _json.loads(json_match.group())
        except _json.JSONDecodeError:
            logger.warning("Content hunter: invalid JSON from AI")
            return

        if not critical_items or not isinstance(critical_items, list):
            return

        now = datetime.now(ALMATY_TZ)
        date_str = now.strftime("%Y-%m-%d")

        for item in critical_items[:3]:  # Макс 3 черновика за раз
            title = item.get("title", "Без заголовка")
            content_type = item.get("type", "article")
            summary = item.get("summary", "")
            urgency = item.get("urgency", "medium")

            # Сохраняем черновик в Content Calendar
            await google.append_content_plan(
                date=date_str,
                content_type=content_type,
                title=f"[DRAFT] {title}",
                status="draft",
            )

            # Отправляем админу уведомление с кнопкой
            urgency_emoji = "🚨" if urgency == "high" else "📰"
            msg_text = (
                f"{urgency_emoji} <b>Контент-хантер нашёл важную новость!</b>\n\n"
                f"<b>{title}</b>\n"
                f"{summary}\n\n"
                f"📋 Черновик добавлен в Content Calendar.\n"
                f"Тип: {content_type}"
            )

            keyboard = InlineKeyboardMarkup(
                inline_keyboard=[
                    [InlineKeyboardButton(
                        text="📝 Опубликовать статью",
                        callback_data="hunter_publish",
                    )],
                    [InlineKeyboardButton(
                        text="📢 Пост в канал",
                        callback_data="hunter_channel",
                    )],
                    [InlineKeyboardButton(
                        text="❌ Пропустить",
                        callback_data="digest_ack",
                    )],
                ]
            )

            try:
                await bot.send_message(
                    chat_id=settings.ADMIN_ID,
                    text=msg_text,
                    reply_markup=keyboard,
                )
            except Exception:
                await bot.send_message(
                    chat_id=settings.ADMIN_ID,
                    text=msg_text,
                    reply_markup=keyboard,
                    parse_mode=None,
                )

        logger.info("Content hunter: найдено %d критических новостей", len(critical_items))

    except Exception as e:
        logger.error("Content hunter error: %s", e)


@router.callback_query(F.data == "hunter_publish")
async def hunter_publish(callback: CallbackQuery) -> None:
    """Быстрая публикация из контент-хантера."""
    if callback.from_user.id != settings.ADMIN_ID:
        return
    await callback.answer()
    await callback.message.answer(
        "📝 Скопируйте заголовок новости и отправьте /publish — "
        "AI автоматически развернёт её в полноценную статью."
    )


@router.callback_query(F.data == "hunter_channel")
async def hunter_channel(callback: CallbackQuery) -> None:
    """Быстрый канальный пост из контент-хантера."""
    if callback.from_user.id != settings.ADMIN_ID:
        return
    await callback.answer()
    await callback.message.answer(
        "📢 Скопируйте заголовок и отправьте мне — сгенерирую пост для канала."
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
            f"💡 <b>Дополнительные идеи:</b>\n\n{response}",
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[
                    [InlineKeyboardButton(text="📝 Опубликовать", callback_data="digest_publish")],
                    [InlineKeyboardButton(text="✅ Достаточно", callback_data="digest_ack")],
                ]
            ),
        )

    except Exception as e:
        await callback.message.answer(f"❌ Ошибка: {e}")
