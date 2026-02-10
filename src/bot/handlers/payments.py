"""Telegram Payments — платные консультации и премиум-гайды.

Продукты:
- Юридическая консультация 30мин (15,000 KZT)
- Премиум гайд M&A (5,000 KZT)
- VIP пакет: гайд + консультация (18,000 KZT)

Хендлеры: pre_checkout_query, successful_payment.
Требует PAYMENT_PROVIDER_TOKEN в .env (Stripe / Kaspi / YooKassa).
"""

import logging
from datetime import datetime, timezone

from aiogram import Bot, F, Router
from aiogram.filters import Command
from aiogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    LabeledPrice,
    Message,
    PreCheckoutQuery,
    SuccessfulPayment,
)

from src.config import settings

router = Router()
logger = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════════════════════════
#  КАТАЛОГ ТОВАРОВ / УСЛУГ
# ═══════════════════════════════════════════════════════════════════════════

PRODUCTS = {
    "consult_30min": {
        "title": "Юридическая консультация 30 мин",
        "description": (
            "Онлайн-консультация с партнёром SOLIS Partners.\n"
            "Корпоративное право, IT-право, МФЦА, M&A."
        ),
        "emoji": "⚖️",
        "prices": [LabeledPrice(label="Консультация 30 мин", amount=1_500_000)],  # 15,000 KZT
        "currency": "KZT",
    },
    "consult_60min": {
        "title": "Юридическая консультация 60 мин",
        "description": "Расширенная онлайн-консультация с детальным анализом.",
        "emoji": "💎",
        "prices": [LabeledPrice(label="Консультация 60 мин", amount=2_500_000)],  # 25,000 KZT
        "currency": "KZT",
    },
    "premium_guide_ma": {
        "title": "Премиум гайд: M&A в Казахстане",
        "description": "Полное руководство по слияниям и поглощениям. 50+ страниц.",
        "emoji": "📚",
        "prices": [LabeledPrice(label="Премиум гайд M&A", amount=500_000)],  # 5,000 KZT
        "currency": "KZT",
    },
    "vip_bundle": {
        "title": "VIP Пакет: Гайд + Консультация",
        "description": "Премиум гайд M&A + 30-минутная консультация. Скидка 10%.",
        "emoji": "👑",
        "prices": [
            LabeledPrice(label="Премиум гайд M&A", amount=500_000),
            LabeledPrice(label="Консультация 30 мин", amount=1_500_000),
            LabeledPrice(label="Скидка VIP", amount=-200_000),
        ],
        "currency": "KZT",
    },
}


# ═══════════════════════════════════════════════════════════════════════════
#  КОМАНДА /shop — Каталог товаров
# ═══════════════════════════════════════════════════════════════════════════


@router.message(Command("shop"))
async def cmd_shop(message: Message) -> None:
    """Показывает каталог платных услуг."""
    text = (
        "🏪 <b>Премиум услуги SOLIS Partners</b>\n\n"
        "Оплата прямо в Telegram — быстро и безопасно.\n"
        "───────────────\n\n"
    )

    buttons = []
    for product_id, product in PRODUCTS.items():
        total = sum(p.amount for p in product["prices"])
        price_str = f"{total // 100:,} ₸".replace(",", " ")
        text += f"{product['emoji']} <b>{product['title']}</b>\n   {price_str}\n\n"
        buttons.append([InlineKeyboardButton(
            text=f"{product['emoji']} {product['title']} — {price_str}",
            callback_data=f"buy_{product_id}",
        )])

    buttons.append([InlineKeyboardButton(
        text="❓ Задать вопрос бесплатно",
        callback_data="start_consult",
    )])

    await message.answer(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))


# ═══════════════════════════════════════════════════════════════════════════
#  СОЗДАНИЕ ИНВОЙСА
# ═══════════════════════════════════════════════════════════════════════════


@router.callback_query(F.data.startswith("buy_"))
async def send_invoice(callback: CallbackQuery, bot: Bot) -> None:
    """Отправляет Telegram Invoice для выбранного продукта."""
    product_id = callback.data.removeprefix("buy_")
    product = PRODUCTS.get(product_id)

    if not product:
        await callback.answer("Товар не найден", show_alert=True)
        return

    token = getattr(settings, "PAYMENT_PROVIDER_TOKEN", "")
    if not token:
        await callback.answer(
            "Платежи временно недоступны. Свяжитесь с @SOLISlegal",
            show_alert=True,
        )
        logger.warning("PAYMENT_PROVIDER_TOKEN not configured")
        return

    try:
        await bot.send_invoice(
            chat_id=callback.from_user.id,
            title=product["title"],
            description=product["description"],
            payload=f"solis_{product_id}_{callback.from_user.id}",
            provider_token=token,
            currency=product["currency"],
            prices=product["prices"],
            start_parameter=f"pay_{product_id}",
            photo_url="https://solispartners.kz/assets/logo.png",
            photo_width=512,
            photo_height=512,
            need_name=True,
            need_phone_number=True,
            need_email=True,
            is_flexible=False,
        )
        await callback.answer()
    except Exception as e:
        logger.error("Invoice send error: %s", e)
        await callback.answer("Ошибка создания счёта. Попробуйте позже.", show_alert=True)


# ═══════════════════════════════════════════════════════════════════════════
#  PRE-CHECKOUT (Telegram обязательный хендлер)
# ═══════════════════════════════════════════════════════════════════════════


@router.pre_checkout_query()
async def process_pre_checkout(pre_checkout: PreCheckoutQuery, bot: Bot) -> None:
    """Подтверждение pre-checkout — проверяем payload и отвечаем OK."""
    payload = pre_checkout.invoice_payload

    if not payload.startswith("solis_"):
        await bot.answer_pre_checkout_query(
            pre_checkout.id, ok=False, error_message="Неизвестный товар."
        )
        return

    # Валидация: проверяем что product_id из payload существует
    parts = payload.removeprefix("solis_").rsplit("_", 1)
    product_id = parts[0] if parts else ""

    if product_id not in PRODUCTS:
        await bot.answer_pre_checkout_query(
            pre_checkout.id, ok=False, error_message="Товар больше не доступен."
        )
        return

    # Всё OK — подтверждаем
    await bot.answer_pre_checkout_query(pre_checkout.id, ok=True)
    logger.info(
        "Pre-checkout OK: user=%s, product=%s, amount=%s",
        pre_checkout.from_user.id, product_id, pre_checkout.total_amount,
    )


# ═══════════════════════════════════════════════════════════════════════════
#  SUCCESSFUL PAYMENT
# ═══════════════════════════════════════════════════════════════════════════


@router.message(F.successful_payment)
async def process_successful_payment(message: Message) -> None:
    """Обработка успешного платежа — выдаём доступ + уведомляем админа."""
    payment: SuccessfulPayment = message.successful_payment
    user_id = message.from_user.id
    payload = payment.invoice_payload

    parts = payload.removeprefix("solis_").rsplit("_", 1)
    product_id = parts[0] if parts else payload
    product = PRODUCTS.get(product_id, {})
    title = product.get("title", product_id)

    total_str = f"{payment.total_amount // 100:,} {payment.currency}".replace(",", " ")

    # Благодарим пользователя
    await message.answer(
        f"✅ <b>Оплата прошла успешно!</b>\n\n"
        f"🛍 {title}\n"
        f"💰 {total_str}\n\n"
        f"───────────────\n\n"
        f"📞 Наш менеджер свяжется с вами в ближайшее время "
        f"для организации консультации.\n\n"
        f"Или напишите сами: @SOLISlegal",
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="📞 Связаться", url="https://t.me/SOLISlegal")],
                [InlineKeyboardButton(text="📚 К гайдам", callback_data="show_all_guides")],
            ]
        ),
    )

    # Карма за покупку
    try:
        from src.bot.utils.karma import add_karma
        add_karma(user_id, 50, "purchase")
    except Exception:
        pass

    # Уведомляем админа
    try:
        name = message.from_user.full_name or ""
        username = message.from_user.username or ""
        phone = payment.order_info.phone_number if payment.order_info else ""
        email = payment.order_info.email if payment.order_info else ""

        await message.bot.send_message(
            chat_id=settings.ADMIN_ID,
            text=(
                f"💰 <b>НОВАЯ ОПЛАТА!</b>\n\n"
                f"👤 {name} (@{username})\n"
                f"📞 {phone}\n"
                f"📧 {email}\n"
                f"🛍 {title}\n"
                f"💰 {total_str}\n"
                f"🆔 <code>{user_id}</code>\n\n"
                f"⚡ Связаться для назначения консультации!"
            ),
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[
                    [InlineKeyboardButton(
                        text=f"💬 Написать @{username}" if username else "📋 Заметка",
                        url=f"https://t.me/{username}" if username else "https://t.me/SOLISlegal",
                    )],
                ]
            ),
        )
    except Exception as e:
        logger.error("Payment admin notification error: %s", e)

    logger.info(
        "Payment OK: user=%s, product=%s, amount=%s %s",
        user_id, product_id, total_str, payment.currency,
    )
