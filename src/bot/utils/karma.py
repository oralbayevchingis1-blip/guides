"""Система «Карма юриста» — геймификация бота.

Начисление баллов:
- Скачивание гайда:       +10
- Чтение статьи:          +5
- AI-консультация:        +3
- Реферал (друг пришёл):  +20
- Покупка:                +50
- NPS-оценка:             +2
- Генерация документа:    +5

Уровни:
- 0-49:    📘 Новичок
- 50-149:  📗 Активный
- 150-299: 📙 Продвинутый
- 300-499: 📕 Эксперт
- 500+:    ⚖️ Мастер права

Использование:
    from src.bot.utils.karma import add_karma, get_karma, get_karma_level
"""

import logging
from collections import defaultdict
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

# Хранилище: {user_id: total_karma}
_karma: dict[int, int] = defaultdict(int)

# Лог начислений: {user_id: [{action, points, ts}, ...]}
_karma_log: dict[int, list[dict]] = defaultdict(list)


# ═══════════════════════════════════════════════════════════════════════════
#  КОНФИГУРАЦИЯ НАЧИСЛЕНИЙ
# ═══════════════════════════════════════════════════════════════════════════

KARMA_ACTIONS = {
    "guide_download": 10,
    "article_read": 5,
    "consult": 3,
    "referral": 20,
    "purchase": 50,
    "nps_feedback": 2,
    "doc_generated": 5,
    "daily_login": 1,
    "share_bot": 5,
    "waitlist_join": 2,
}


# ═══════════════════════════════════════════════════════════════════════════
#  УРОВНИ
# ═══════════════════════════════════════════════════════════════════════════

KARMA_LEVELS = [
    {"min": 0, "name": "Новичок", "emoji": "📘"},
    {"min": 50, "name": "Активный", "emoji": "📗"},
    {"min": 150, "name": "Продвинутый", "emoji": "📙"},
    {"min": 300, "name": "Эксперт", "emoji": "📕"},
    {"min": 500, "name": "Мастер права", "emoji": "⚖️"},
]


# ═══════════════════════════════════════════════════════════════════════════
#  PUBLIC API
# ═══════════════════════════════════════════════════════════════════════════


def add_karma(user_id: int, points: int = 0, action: str = "") -> int:
    """Начисляет карму пользователю.

    Args:
        user_id: Telegram user ID.
        points: Количество баллов (если 0 — берётся из KARMA_ACTIONS по action).
        action: Тип действия.

    Returns:
        Новый общий счёт кармы.
    """
    if points == 0 and action in KARMA_ACTIONS:
        points = KARMA_ACTIONS[action]

    if points <= 0:
        return _karma[user_id]

    _karma[user_id] += points
    _karma_log[user_id].append({
        "action": action,
        "points": points,
        "ts": datetime.now(timezone.utc).isoformat(),
    })

    # Ограничиваем лог 100 записями
    if len(_karma_log[user_id]) > 100:
        _karma_log[user_id] = _karma_log[user_id][-100:]

    logger.debug("Karma +%d for user=%s (%s). Total: %d",
                 points, user_id, action, _karma[user_id])
    return _karma[user_id]


def get_karma(user_id: int) -> int:
    """Текущая карма пользователя."""
    return _karma.get(user_id, 0)


def get_karma_level(user_id: int) -> dict:
    """Уровень пользователя на основе кармы."""
    karma = get_karma(user_id)
    level = KARMA_LEVELS[0]
    for lvl in KARMA_LEVELS:
        if karma >= lvl["min"]:
            level = lvl
    return level


def get_karma_next_level(user_id: int) -> dict | None:
    """Следующий уровень (для прогресса)."""
    karma = get_karma(user_id)
    for lvl in KARMA_LEVELS:
        if lvl["min"] > karma:
            return lvl
    return None


def get_karma_profile(user_id: int) -> str:
    """HTML-карточка кармы пользователя."""
    karma = get_karma(user_id)
    level = get_karma_level(user_id)
    next_lvl = get_karma_next_level(user_id)

    text = (
        f"{level['emoji']} <b>Уровень: {level['name']}</b>\n"
        f"⭐ Карма: <b>{karma}</b> баллов\n"
    )

    if next_lvl:
        remaining = next_lvl["min"] - karma
        # Прогресс-бар
        prev_min = level["min"]
        total_range = next_lvl["min"] - prev_min
        progress = karma - prev_min
        bar_len = 10
        filled = int(progress / total_range * bar_len) if total_range > 0 else 0
        bar = "█" * filled + "░" * (bar_len - filled)

        text += (
            f"\n📊 Прогресс: [{bar}]\n"
            f"   До {next_lvl['emoji']} {next_lvl['name']}: <b>{remaining}</b> баллов\n"
        )
    else:
        text += "\n🏆 <b>Максимальный уровень!</b>\n"

    return text


def get_karma_leaderboard(limit: int = 10) -> list[dict]:
    """Топ пользователей по карме."""
    sorted_users = sorted(_karma.items(), key=lambda x: x[1], reverse=True)[:limit]
    result = []
    for rank, (uid, karma) in enumerate(sorted_users, 1):
        level = get_karma_level(uid)
        result.append({
            "rank": rank,
            "user_id": uid,
            "karma": karma,
            "level": level["name"],
            "emoji": level["emoji"],
        })
    return result


def get_karma_log(user_id: int, limit: int = 10) -> list[dict]:
    """Последние начисления кармы."""
    return _karma_log.get(user_id, [])[-limit:]
