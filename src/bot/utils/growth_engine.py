"""Growth Engine — реферальные milestones, A/B тесты, UTM трекинг, CRM webhooks.

Модули:
- Referral Milestones: уровни достижений за рефералов
- A/B Testing: динамическое тестирование офферов
- UTM/Partner Tracking: фиксация партнёрских лидов
- CRM Webhook: отправка HOT лидов во внешнюю CRM
- Smart Broadcast: сегментация рассылок по интересам

Использование:
    from src.bot.utils.growth_engine import (
        check_referral_milestone, get_ab_variant,
        parse_utm_source, send_crm_webhook, segment_users,
    )
"""

import asyncio
import hashlib
import json
import logging
import time
from collections import defaultdict
from datetime import datetime, timezone

import aiohttp

from src.config import settings

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════════════════
#  1. REFERRAL MILESTONES
# ═══════════════════════════════════════════════════════════════════════════

# Уровни достижений
REFERRAL_MILESTONES = [
    {"count": 1, "reward": "first_friend", "emoji": "🤝", "text": "Первый друг! Спасибо за рекомендацию."},
    {"count": 3, "reward": "gold_guide", "emoji": "⭐", "text": "Золотой гайд разблокирован! Эксклюзивный материал по M&A."},
    {"count": 5, "reward": "priority_support", "emoji": "💎", "text": "Приоритетная поддержка! Ваши вопросы обрабатываются первыми."},
    {"count": 10, "reward": "free_consult", "emoji": "🏆", "text": "Бесплатная 15-минутная консультация с юристом SOLIS Partners!"},
    {"count": 25, "reward": "vip_partner", "emoji": "👑", "text": "VIP-партнёр SOLIS! Персональный менеджер и скидка 20%."},
]


def check_referral_milestone(ref_count: int) -> dict | None:
    """Проверяет, достигнут ли новый milestone.

    Args:
        ref_count: Текущее количество рефералов.

    Returns:
        Milestone dict или None, если milestone не достигнут.
    """
    for ms in REFERRAL_MILESTONES:
        if ref_count == ms["count"]:
            return ms
    return None


def get_next_milestone(ref_count: int) -> dict | None:
    """Возвращает следующий milestone, который нужно достичь."""
    for ms in REFERRAL_MILESTONES:
        if ms["count"] > ref_count:
            return ms
    return None


def referral_progress_text(ref_count: int) -> str:
    """Генерирует текст прогресса по реферальной программе (HTML)."""
    lines = []
    for ms in REFERRAL_MILESTONES:
        if ref_count >= ms["count"]:
            lines.append(f"  ✅ {ms['emoji']} {ms['text'].split('!')[0]}!")
        else:
            remaining = ms["count"] - ref_count
            lines.append(f"  🔒 {ms['emoji']} Ещё {remaining} до: {ms['text'].split('!')[0]}")
    return "\n".join(lines)


# ═══════════════════════════════════════════════════════════════════════════
#  2. A/B TESTING
# ═══════════════════════════════════════════════════════════════════════════

# Хранилище A/B экспериментов: {test_id: {variant_a: clicks, variant_b: clicks}}
_ab_experiments: dict[str, dict] = {}

# Время создания экспериментов
_ab_created: dict[str, float] = {}

# Результаты (кто какой вариант видел)
_ab_assignments: dict[str, dict[int, str]] = defaultdict(dict)

# Период эксперимента (24 часа)
AB_EXPERIMENT_DURATION = 86400


def get_ab_variant(test_id: str, user_id: int) -> str:
    """Определяет вариант A/B теста для пользователя.

    Детерминированное распределение через hash(user_id + test_id).

    Args:
        test_id: Идентификатор эксперимента (e.g. "welcome_text_v2").
        user_id: Telegram user ID.

    Returns:
        "A" или "B"
    """
    # Инициализируем эксперимент если новый
    if test_id not in _ab_experiments:
        _ab_experiments[test_id] = {"A": 0, "B": 0, "A_conv": 0, "B_conv": 0}
        _ab_created[test_id] = time.time()

    # Проверяем, не истёк ли эксперимент
    elapsed = time.time() - _ab_created.get(test_id, 0)
    if elapsed > AB_EXPERIMENT_DURATION:
        winner = get_ab_winner(test_id)
        if winner:
            return winner

    # Детерминированное распределение
    h = hashlib.md5(f"{user_id}:{test_id}".encode()).hexdigest()
    variant = "A" if int(h, 16) % 2 == 0 else "B"

    _ab_assignments[test_id][user_id] = variant
    _ab_experiments[test_id][variant] += 1

    return variant


def record_ab_conversion(test_id: str, user_id: int) -> None:
    """Записывает конверсию для A/B теста."""
    if test_id not in _ab_experiments:
        return
    variant = _ab_assignments.get(test_id, {}).get(user_id)
    if variant:
        _ab_experiments[test_id][f"{variant}_conv"] += 1


def get_ab_winner(test_id: str) -> str | None:
    """Определяет победителя A/B теста.

    Returns:
        "A", "B" или None если недостаточно данных.
    """
    exp = _ab_experiments.get(test_id)
    if not exp:
        return None

    a_views = exp.get("A", 0)
    b_views = exp.get("B", 0)
    a_conv = exp.get("A_conv", 0)
    b_conv = exp.get("B_conv", 0)

    if a_views < 5 or b_views < 5:
        return None  # Недостаточно данных

    rate_a = a_conv / a_views if a_views > 0 else 0
    rate_b = b_conv / b_views if b_views > 0 else 0

    return "A" if rate_a >= rate_b else "B"


def get_ab_stats(test_id: str) -> dict:
    """Статистика A/B теста."""
    exp = _ab_experiments.get(test_id, {})
    a_views = exp.get("A", 0)
    b_views = exp.get("B", 0)
    a_conv = exp.get("A_conv", 0)
    b_conv = exp.get("B_conv", 0)
    return {
        "test_id": test_id,
        "A_views": a_views,
        "B_views": b_views,
        "A_conversions": a_conv,
        "B_conversions": b_conv,
        "A_rate": round(a_conv / a_views * 100, 1) if a_views else 0,
        "B_rate": round(b_conv / b_views * 100, 1) if b_views else 0,
        "winner": get_ab_winner(test_id),
        "elapsed_hours": round((time.time() - _ab_created.get(test_id, time.time())) / 3600, 1),
    }


# ═══════════════════════════════════════════════════════════════════════════
#  3. SMART BROADCAST SEGMENTATION
# ═══════════════════════════════════════════════════════════════════════════

# Маппинг guide_id -> тематические теги
GUIDE_INTEREST_MAP: dict[str, list[str]] = {
    "too": ["corporate", "registration", "business"],
    "ip": ["startup", "registration", "business"],
    "mfca": ["aifc", "international", "finance"],
    "aifc": ["aifc", "international", "finance"],
    "esop": ["startup", "corporate", "finance"],
    "taxes": ["tax", "finance", "business"],
    "labor": ["labor", "hr", "business"],
    "it_law": ["it", "tech", "ip"],
    "ma": ["corporate", "finance", "m&a"],
}


def get_user_interests(leads: list[dict], user_id: int) -> set[str]:
    """Определяет интересы пользователя по скачанным гайдам."""
    interests: set[str] = set()
    for lead in leads:
        if str(lead.get("user_id", "")) == str(user_id):
            guide = str(lead.get("guide", lead.get("selected_guide", ""))).lower()
            for key, tags in GUIDE_INTEREST_MAP.items():
                if key in guide:
                    interests.update(tags)
    return interests


def segment_users(
    all_leads: list[dict],
    user_ids: list[int],
    target_tags: list[str],
) -> list[int]:
    """Фильтрует пользователей по интересам.

    Args:
        all_leads: Все лиды из Sheets.
        user_ids: Все user_id.
        target_tags: Теги целевой аудитории (e.g. ["it", "tech"]).

    Returns:
        Список user_id, чьи интересы пересекаются с target_tags.
    """
    target_set = set(t.lower() for t in target_tags)
    matched = []

    for uid in user_ids:
        interests = get_user_interests(all_leads, uid)
        if interests & target_set:
            matched.append(uid)

    return matched


# ═══════════════════════════════════════════════════════════════════════════
#  4. CRM WEBHOOK (Pipedrive/HubSpot)
# ═══════════════════════════════════════════════════════════════════════════

# URL вебхука настраивается через env (пустой = отключен)
CRM_WEBHOOK_URL = getattr(settings, "CRM_WEBHOOK_URL", "")


async def send_crm_webhook(lead_data: dict) -> bool:
    """Отправляет данные HOT-лида во внешнюю CRM через webhook.

    Args:
        lead_data: Словарь с данными лида.

    Returns:
        True если отправлено успешно.
    """
    url = CRM_WEBHOOK_URL
    if not url:
        logger.debug("CRM webhook disabled (no URL configured)")
        return False

    payload = {
        "source": "solis_telegram_bot",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "lead": {
            "user_id": lead_data.get("user_id"),
            "name": lead_data.get("name", ""),
            "email": lead_data.get("email", ""),
            "phone": lead_data.get("phone", ""),
            "username": lead_data.get("username", ""),
            "score": lead_data.get("score", 0),
            "label": lead_data.get("label", ""),
            "interests": lead_data.get("interests", []),
            "source": lead_data.get("source", "telegram"),
            "partner_id": lead_data.get("partner_id", ""),
        },
    }

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                url,
                json=payload,
                timeout=aiohttp.ClientTimeout(total=10),
            ) as resp:
                if resp.status < 300:
                    logger.info("CRM webhook sent: user_id=%s", lead_data.get("user_id"))
                    return True
                else:
                    body = await resp.text()
                    logger.warning("CRM webhook %d: %s", resp.status, body[:200])
                    return False
    except Exception as e:
        logger.error("CRM webhook error: %s", e)
        return False


# ═══════════════════════════════════════════════════════════════════════════
#  9. UTM / PARTNER TRACKING
# ═══════════════════════════════════════════════════════════════════════════


def parse_utm_source(args: str) -> dict:
    """Парсит deep-link параметр в структурированные UTM-данные.

    Форматы:
        - ref_{user_id}            → referral
        - partner_{id}_{campaign}  → partner traffic
        - utm_{source}_{medium}    → UTM-метка
        - {source}                 → простой источник (instagram, linkedin, etc.)

    Args:
        args: Аргумент из /start command.

    Returns:
        {"type": ..., "source": ..., "partner_id": ..., "campaign": ...}
    """
    if not args:
        return {"type": "direct", "source": "direct", "partner_id": "", "campaign": ""}

    args = args.strip()

    # Реферальная ссылка
    if args.startswith("ref_"):
        return {
            "type": "referral",
            "source": "referral",
            "partner_id": "",
            "campaign": "",
            "referrer_id": args.removeprefix("ref_"),
        }

    # Партнёрская ссылка: partner_{id}_{campaign}
    if args.startswith("partner_"):
        parts = args.removeprefix("partner_").split("_", 1)
        partner_id = parts[0] if parts else ""
        campaign = parts[1] if len(parts) > 1 else ""
        return {
            "type": "partner",
            "source": f"partner_{partner_id}",
            "partner_id": partner_id,
            "campaign": campaign,
        }

    # UTM-метка: utm_{source}_{medium}
    if args.startswith("utm_"):
        parts = args.removeprefix("utm_").split("_", 1)
        source = parts[0] if parts else args
        medium = parts[1] if len(parts) > 1 else ""
        return {
            "type": "utm",
            "source": source,
            "partner_id": "",
            "campaign": medium,
        }

    # Простой источник (instagram, linkedin, channel_pin, etc.)
    return {
        "type": "organic",
        "source": args,
        "partner_id": "",
        "campaign": "",
    }
