"""Обработчик команды /start — точка входа в воронку."""

import asyncio
import logging

from aiogram import Bot, Router
from aiogram.filters import CommandStart, CommandObject
from aiogram.fsm.context import FSMContext
from aiogram.types import Message

from src.bot.keyboards.inline import guides_menu_keyboard, subscription_keyboard
from src.bot.utils.cache import TTLCache
from src.bot.utils.google_sheets import GoogleSheetsClient
from src.bot.utils.subscription_check import check_subscription
from src.constants import get_text
from src.database.crud import get_or_create_user, save_referral

router = Router()
logger = logging.getLogger(__name__)


@router.message(CommandStart())
async def cmd_start(
    message: Message,
    bot: Bot,
    state: FSMContext,
    command: CommandObject,
    google: GoogleSheetsClient,
    cache: TTLCache,
) -> None:
    """Обработка команды /start.

    1. Парсит deep-link параметр (источник трафика).
    2. Сохраняет/обновляет пользователя в БД.
    3. Загружает тексты и каталог из Google Sheets (через кеш).
    4. Проверяет подписку на канал.
    5. Если не подписан — показывает кнопку подписки.
    6. Если подписан — показывает меню гайдов.

    Deep-link примеры:
        t.me/SOLIS_Partners_Legal_bot?start=instagram
        t.me/SOLIS_Partners_Legal_bot?start=linkedin_post_feb
        t.me/SOLIS_Partners_Legal_bot?start=channel_pin
    """
    user = message.from_user
    if user is None:
        return

    # Парсим deep-link параметр (UTM / partner / referral)
    from src.bot.utils.growth_engine import parse_utm_source

    raw_source = command.args or ""
    utm = parse_utm_source(raw_source)
    source = utm.get("source", raw_source)
    partner_id = utm.get("partner_id", "")

    if raw_source:
        logger.info(
            "Deep-link: '%s' → type=%s, source=%s, partner=%s (user_id=%s)",
            raw_source, utm["type"], source, partner_id, user.id,
        )

    # Сохраняем пользователя в БД (SQLite backup)
    db_user = await get_or_create_user(
        user_id=user.id,
        username=user.username,
        full_name=user.full_name,
    )

    # Сохраняем partner_id и source если пришёл от партнёра
    if partner_id or utm["type"] != "direct":
        try:
            from src.database.models import async_session as _asession
            async with _asession() as session:
                from sqlalchemy import select
                from src.database.models import User as UserModel
                stmt = select(UserModel).where(UserModel.user_id == user.id)
                result = await session.execute(stmt)
                u = result.scalar_one_or_none()
                if u:
                    if partner_id and not u.partner_id:
                        u.partner_id = partner_id
                    if source and not u.traffic_source:
                        u.traffic_source = source
                    await session.commit()
        except Exception:
            pass

    logger.info("Команда /start от user_id=%s", user.id)

    # P5: Телеметрия
    try:
        from src.bot.utils.telemetry import track_event
        asyncio.create_task(track_event(user.id, "bot_started", {
            "source": source, "type": utm["type"],
        }))
    except Exception:
        pass

    # Обработка реферальной ссылки (ref_{user_id})
    if utm["type"] == "referral":
        try:
            referrer_id = int(utm.get("referrer_id", "0"))
            was_saved = await save_referral(
                referrer_id=referrer_id,
                referred_id=user.id,
            )
            if was_saved:
                # Импортируем здесь чтобы избежать циклической зависимости
                from src.bot.handlers.referral import notify_referrer

                asyncio.create_task(
                    notify_referrer(
                        bot,
                        referrer_id=referrer_id,
                        new_user_name=user.full_name or user.username or "Новый пользователь",
                    )
                )
                logger.info("Реферал: %s привёл %s", referrer_id, user.id)
        except (ValueError, TypeError):
            logger.warning("Невалидный реферальный ID: '%s'", source)

    # Сброс состояния FSM (на случай повторного /start)
    await state.clear()

    # Сохраняем source + partner в FSM state для записи лида
    if source:
        await state.update_data(
            traffic_source=source,
            partner_id=partner_id,
            utm_type=utm["type"],
            utm_campaign=utm.get("campaign", ""),
        )

    # Автодетекция языка по Telegram language_code
    from src.bot.utils.i18n import get_user_lang, set_user_lang
    tg_lang = user.language_code or ""
    user_lang = get_user_lang(user.id, tg_lang)
    set_user_lang(user.id, user_lang)

    # Загружаем тексты и каталог из Google Sheets (с кешем)
    texts = await cache.get_or_fetch("texts", google.get_bot_texts)
    catalog = await cache.get_or_fetch("catalog", google.get_guides_catalog)

    # Проверка подписки
    is_subscribed = await check_subscription(user.id, bot)

    if not is_subscribed:
        await message.answer(
            get_text(texts, "welcome_not_subscribed"),
            reply_markup=subscription_keyboard(),
        )
    else:
        try:
            await message.answer(
                get_text(texts, "welcome_subscribed"),
                reply_markup=guides_menu_keyboard(catalog),
            )
        except Exception as e:
            logger.error("Ошибка отображения каталога: %s", e)
            await message.answer(
                get_text(texts, "welcome_subscribed")
                + "\n\n⚠️ Ошибка загрузки каталога. Попробуйте /start снова.",
            )
