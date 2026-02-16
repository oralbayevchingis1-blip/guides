"""C4. Email-маркетинг (SMTP/Resend Integration).

Отправка приветственных писем, дожим лидов, уведомления.
Поддерживает SMTP и Resend API (если настроен).

C5. Sentiment Analysis — определение срочности вопроса.
"""

import asyncio
import logging
import re
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Optional

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════════════════
#  C4: Email Sender
# ═══════════════════════════════════════════════════════════════════════════

# Настройки из env (добавляются в settings)
_EMAIL_CONFIGURED = False


def _get_email_config() -> dict:
    """Загружает конфигурацию email из settings."""
    from src.config import settings
    return {
        "smtp_host": getattr(settings, "SMTP_HOST", ""),
        "smtp_port": int(getattr(settings, "SMTP_PORT", 587)),
        "smtp_user": getattr(settings, "SMTP_USER", ""),
        "smtp_password": getattr(settings, "SMTP_PASSWORD", ""),
        "from_email": getattr(settings, "FROM_EMAIL", "noreply@solispartners.kz"),
        "from_name": getattr(settings, "FROM_NAME", "SOLIS Partners"),
        "resend_api_key": getattr(settings, "RESEND_API_KEY", ""),
    }


async def send_email(
    to_email: str,
    subject: str,
    html_body: str,
    text_body: str = "",
) -> bool:
    """Отправляет email через SMTP или Resend API.

    Returns:
        True если отправлено успешно.
    """
    config = _get_email_config()

    # Попытка через Resend API
    if config["resend_api_key"]:
        return await _send_via_resend(to_email, subject, html_body, config)

    # Попытка через SMTP
    if config["smtp_host"] and config["smtp_user"]:
        return await _send_via_smtp(to_email, subject, html_body, text_body, config)

    logger.info("Email not configured — skipping send to %s", to_email[:20])
    return False


async def _send_via_smtp(
    to_email: str, subject: str, html_body: str, text_body: str, config: dict,
) -> bool:
    """Отправка через SMTP."""
    def _send():
        try:
            msg = MIMEMultipart("alternative")
            msg["Subject"] = subject
            msg["From"] = f"{config['from_name']} <{config['from_email']}>"
            msg["To"] = to_email

            if text_body:
                msg.attach(MIMEText(text_body, "plain", "utf-8"))
            msg.attach(MIMEText(html_body, "html", "utf-8"))

            with smtplib.SMTP(config["smtp_host"], config["smtp_port"]) as server:
                server.starttls()
                server.login(config["smtp_user"], config["smtp_password"])
                server.send_message(msg)

            logger.info("Email sent via SMTP to %s", to_email)
            return True
        except Exception as e:
            logger.error("SMTP send failed: %s", e)
            return False

    return await asyncio.to_thread(_send)


async def _send_via_resend(
    to_email: str, subject: str, html_body: str, config: dict,
) -> bool:
    """Отправка через Resend API."""
    try:
        import aiohttp
        async with aiohttp.ClientSession() as session:
            resp = await session.post(
                "https://api.resend.com/emails",
                headers={
                    "Authorization": f"Bearer {config['resend_api_key']}",
                    "Content-Type": "application/json",
                },
                json={
                    "from": f"{config['from_name']} <{config['from_email']}>",
                    "to": [to_email],
                    "subject": subject,
                    "html": html_body,
                },
            )
            if resp.status in (200, 201):
                logger.info("Email sent via Resend to %s", to_email)
                return True
            else:
                err = await resp.text()
                logger.error("Resend error %d: %s", resp.status, err[:200])
                return False
    except Exception as e:
        logger.error("Resend send failed: %s", e)
        return False


async def send_welcome_email(name: str, email: str, guide_name: str = "") -> bool:
    """Отправляет приветственное письмо новому лиду."""
    subject = f"Добро пожаловать в SOLIS Partners, {name}!"
    html = f"""
    <div style="font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto;">
        <div style="background: #1a237e; padding: 20px; text-align: center;">
            <h1 style="color: #c9a227; margin: 0;">SOLIS Partners</h1>
            <p style="color: #fff; margin: 5px 0;">Юридическая фирма нового поколения</p>
        </div>
        <div style="padding: 30px; background: #f5f5f5;">
            <h2 style="color: #1a237e;">Здравствуйте, {name}!</h2>
            <p>Спасибо за интерес к материалам SOLIS Partners.</p>
            {"<p>Ваш гайд <b>«" + guide_name + "»</b> уже ждёт вас в Telegram-боте.</p>" if guide_name else ""}
            <p>Что мы можем для вас:</p>
            <ul>
                <li>🤖 <b>AI-юрист</b> — бесплатная мини-консультация 24/7</li>
                <li>📄 <b>Генератор документов</b> — NDA, договоры, претензии</li>
                <li>📚 <b>Библиотека гайдов</b> — практические руководства</li>
            </ul>
            <p style="text-align: center; margin-top: 30px;">
                <a href="https://t.me/SOLIS_Partners_Legal_bot" 
                   style="background: #c9a227; color: #fff; padding: 12px 30px; 
                          text-decoration: none; border-radius: 5px; font-weight: bold;">
                    Открыть бота →
                </a>
            </p>
        </div>
        <div style="padding: 15px; text-align: center; color: #999; font-size: 12px;">
            <p>© SOLIS Partners | <a href="https://solispartners.kz">solispartners.kz</a></p>
            <p>Вы получили это письмо, потому что оставили заявку через нашего бота.</p>
        </div>
    </div>
    """
    return await send_email(email, subject, html)


# ═══════════════════════════════════════════════════════════════════════════
#  C5: Sentiment Analysis — определение срочности
# ═══════════════════════════════════════════════════════════════════════════

# Слова-триггеры для CRITICAL
CRITICAL_KEYWORDS = [
    "суд", "обыск", "арест", "блокировка счета", "блокировка счёта",
    "задержание", "допрос", "уголовное", "изъятие", "налоговая проверка",
    "принудительное", "исполнительное производство", "ликвидация",
    "банкротство", "штраф", "санкция", "антимонопольн",
]

# Слова-триггеры для URGENT
URGENT_KEYWORDS = [
    "срочно", "помогите", "что делать", "как быть", "паника",
    "завтра суд", "послезавтра", "через час", "через день",
    "немедленно", "экстренно", "спасите", "катастрофа",
    "вчера", "просрочен", "истёк срок", "истек срок",
]

# Слова-триггеры для HIGH
HIGH_KEYWORDS = [
    "важно", "приоритет", "быстро", "скоро дедлайн",
    "не успеваю", "горит", "подскажите срочно",
]

# Негативные эмоциональные маркеры
NEGATIVE_EMOTION = [
    "ужас", "кошмар", "беда", "проблема", "опасност",
    "угроз", "страш", "потеря", "не знаю что делать",
]


def analyze_sentiment(text: str) -> dict:
    """Анализирует эмоциональный фон и срочность сообщения.

    Returns:
        {
            "urgency": "CRITICAL" | "URGENT" | "HIGH" | "NORMAL",
            "triggers": list[str],  # слова-триггеры
            "emotion": "panic" | "negative" | "neutral",
            "score": int,  # 0-100, чем выше — тем срочнее
            "needs_alert": bool,  # True если нужно алертить админа
        }
    """
    text_lower = text.lower()
    triggers = []
    score = 0

    # CRITICAL — самый высокий приоритет
    for kw in CRITICAL_KEYWORDS:
        if kw in text_lower:
            triggers.append(kw)
            score += 25

    # URGENT
    for kw in URGENT_KEYWORDS:
        if kw in text_lower:
            triggers.append(kw)
            score += 15

    # HIGH
    for kw in HIGH_KEYWORDS:
        if kw in text_lower:
            triggers.append(kw)
            score += 8

    # Негативные эмоции
    emotion = "neutral"
    for em in NEGATIVE_EMOTION:
        if em in text_lower:
            emotion = "negative"
            score += 5
            break

    # Восклицательные знаки и CAPS
    excl_count = text.count("!") + text.count("?!")
    if excl_count >= 3:
        score += 10
        emotion = "panic" if emotion == "negative" else emotion

    caps_ratio = sum(1 for c in text if c.isupper()) / max(len(text), 1)
    if caps_ratio > 0.4 and len(text) > 20:
        score += 10

    # Определяем уровень срочности
    score = min(score, 100)
    if score >= 50:
        urgency = "CRITICAL"
    elif score >= 30:
        urgency = "URGENT"
    elif score >= 15:
        urgency = "HIGH"
    else:
        urgency = "NORMAL"

    return {
        "urgency": urgency,
        "triggers": list(set(triggers))[:5],
        "emotion": emotion,
        "score": score,
        "needs_alert": urgency in ("CRITICAL", "URGENT"),
    }


async def send_urgency_alert(
    bot,
    user_id: int,
    question: str,
    sentiment: dict,
) -> None:
    """Отправляет экстренный алерт админу при обнаружении срочности."""
    from src.config import settings

    urgency = sentiment["urgency"]
    emoji = "🚨" if urgency == "CRITICAL" else "⚠️"
    triggers = ", ".join(sentiment["triggers"][:5]) or "—"

    text = (
        f"{emoji} <b>{urgency} ALERT</b>\n\n"
        f"👤 User ID: <code>{user_id}</code>\n"
        f"📊 Score: {sentiment['score']}/100\n"
        f"🔑 Триггеры: {triggers}\n"
        f"💬 Эмоция: {sentiment['emotion']}\n\n"
        f"📝 <b>Вопрос:</b>\n<i>{question[:500]}</i>"
    )

    try:
        from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(
                text="📞 Ответить клиенту",
                callback_data=f"reply_user_{user_id}",
            )],
        ])
        await bot.send_message(settings.ADMIN_ID, text, reply_markup=keyboard)
    except Exception as e:
        logger.error("Urgency alert failed: %s", e)
