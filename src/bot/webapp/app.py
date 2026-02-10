"""Telegram Mini App (TWA) — FastAPI-дашборд для админа.

Открывается внутри Telegram через WebAppInfo.
Отображает: карточки статей, графики лидов, Data Room, кarma пользователей.

Запуск: uvicorn src.bot.webapp.app:app --port 8443
"""

import json
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

logger = logging.getLogger(__name__)

app = FastAPI(title="SOLIS Partners Admin Dashboard", version="1.0")


# ═══════════════════════════════════════════════════════════════════════════
#  API ENDPOINTS (JSON для Mini App frontend)
# ═══════════════════════════════════════════════════════════════════════════


@app.get("/api/stats")
async def api_stats():
    """Основные метрики для дашборда."""
    try:
        from src.database.models import async_session, User, Lead, Referral
        from sqlalchemy import select, func as sa_func

        now = datetime.now(timezone.utc)
        week_ago = now - timedelta(days=7)

        async with async_session() as session:
            total_users = (await session.execute(
                select(sa_func.count(User.id))
            )).scalar() or 0

            active_7d = (await session.execute(
                select(sa_func.count(User.id)).where(User.last_activity >= week_ago)
            )).scalar() or 0

            total_leads = (await session.execute(
                select(sa_func.count(Lead.id))
            )).scalar() or 0

            total_referrals = (await session.execute(
                select(sa_func.count(Referral.id))
            )).scalar() or 0

        return {
            "total_users": total_users,
            "active_7d": active_7d,
            "total_leads": total_leads,
            "total_referrals": total_referrals,
            "retention_pct": round(active_7d / total_users * 100, 1) if total_users else 0,
            "timestamp": now.isoformat(),
        }
    except Exception as e:
        logger.error("Stats API error: %s", e)
        return {"error": str(e)}


@app.get("/api/leads")
async def api_leads(limit: int = 50):
    """Последние лиды."""
    try:
        from src.database.models import async_session, Lead
        from sqlalchemy import select

        async with async_session() as session:
            result = await session.execute(
                select(Lead).order_by(Lead.id.desc()).limit(limit)
            )
            leads = result.scalars().all()
            return [
                {
                    "id": l.id,
                    "user_id": l.user_id,
                    "email": l.email,
                    "name": l.name,
                    "guide": l.selected_guide,
                    "created_at": l.created_at.isoformat() if l.created_at else "",
                }
                for l in leads
            ]
    except Exception as e:
        return {"error": str(e)}


@app.get("/api/karma/top")
async def api_karma_top(limit: int = 20):
    """Топ пользователей по карме."""
    try:
        from src.bot.utils.karma import get_karma_leaderboard
        return get_karma_leaderboard(limit)
    except Exception as e:
        return {"error": str(e)}


@app.get("/api/ab_tests")
async def api_ab_tests():
    """Статистика A/B тестов."""
    try:
        from src.bot.utils.growth_engine import _ab_experiments, get_ab_stats
        results = {}
        for test_id in _ab_experiments:
            results[test_id] = get_ab_stats(test_id)
        return results
    except Exception as e:
        return {"error": str(e)}


@app.get("/api/waitlists")
async def api_waitlists():
    """Активные waitlists."""
    try:
        from src.bot.utils.waitlist import get_all_waitlists
        return get_all_waitlists()
    except Exception as e:
        return {"error": str(e)}


# ═══════════════════════════════════════════════════════════════════════════
#  L8: Цифровое дело — личный кабинет клиента
# ═══════════════════════════════════════════════════════════════════════════


@app.get("/api/user/{user_id}/documents")
async def api_user_documents(user_id: int):
    """Документы пользователя (сгенерированные через бота)."""
    try:
        from pathlib import Path

        docs_dir = Path("data/generated_docs")
        user_docs = []

        if docs_dir.exists():
            for f in docs_dir.iterdir():
                if f.is_file() and str(user_id) in f.name:
                    user_docs.append({
                        "filename": f.name,
                        "size": f.stat().st_size,
                        "created": datetime.fromtimestamp(f.stat().st_mtime, tz=timezone.utc).isoformat(),
                    })

        return {"user_id": user_id, "documents": user_docs}
    except Exception as e:
        return {"error": str(e)}


@app.get("/api/user/{user_id}/consultations")
async def api_user_consultations(user_id: int, limit: int = 20):
    """История консультаций пользователя."""
    try:
        from src.bot.handlers.live_support import _ai_history

        history = _ai_history.get(user_id, [])
        return {
            "user_id": user_id,
            "consultations": history[-limit:],
            "total": len(history),
        }
    except Exception as e:
        return {"error": str(e)}


@app.get("/api/user/{user_id}/profile")
async def api_user_profile(user_id: int):
    """Полный профиль пользователя для Mini App."""
    try:
        from src.database.models import async_session, User, Lead
        from sqlalchemy import select, func as sa_func
        from src.bot.utils.karma import get_karma, get_karma_level

        async with async_session() as session:
            user = (await session.execute(
                select(User).where(User.user_id == user_id)
            )).scalar_one_or_none()

            lead_count = (await session.execute(
                select(sa_func.count(Lead.id)).where(Lead.user_id == user_id)
            )).scalar() or 0

        if not user:
            return {"error": "User not found"}

        return {
            "user_id": user_id,
            "username": user.username or "",
            "full_name": user.full_name or "",
            "karma": get_karma(user_id),
            "level": get_karma_level(user_id),
            "leads": lead_count,
            "timezone": user.timezone or "Asia/Almaty",
            "language": user.language or "ru",
            "joined": user.created_at.isoformat() if user.created_at else "",
        }
    except Exception as e:
        return {"error": str(e)}


@app.get("/api/tickets")
async def api_tickets():
    """Открытые тикеты (для Mini App)."""
    try:
        from src.bot.utils.ticket_manager import get_open_tickets
        return get_open_tickets()
    except Exception as e:
        return {"error": str(e)}


# ═══════════════════════════════════════════════════════════════════════════
#  MINI APP HTML (Single Page)
# ═══════════════════════════════════════════════════════════════════════════


DASHBOARD_HTML = """<!DOCTYPE html>
<html lang="ru">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>SOLIS Partners Dashboard</title>
<script src="https://telegram.org/js/telegram-web-app.js"></script>
<style>
:root {
    --bg: #1a1a2e;
    --card: #16213e;
    --accent: #c9a227;
    --text: #eaeaea;
    --muted: #8892b0;
    --success: #4ade80;
    --danger: #f87171;
}
* { margin: 0; padding: 0; box-sizing: border-box; }
body {
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
    background: var(--bg);
    color: var(--text);
    padding: 16px;
    min-height: 100vh;
}
h1 { color: var(--accent); font-size: 1.4em; margin-bottom: 16px; text-align: center; }
h2 { color: var(--accent); font-size: 1.1em; margin: 16px 0 8px; }
.grid { display: grid; grid-template-columns: 1fr 1fr; gap: 12px; margin-bottom: 16px; }
.card {
    background: var(--card);
    border-radius: 12px;
    padding: 16px;
    border: 1px solid rgba(201,162,39,0.15);
}
.card .label { color: var(--muted); font-size: 0.8em; text-transform: uppercase; }
.card .value { font-size: 1.8em; font-weight: bold; color: var(--accent); margin-top: 4px; }
.card .sub { font-size: 0.75em; color: var(--muted); margin-top: 4px; }
.leads-table { width: 100%; border-collapse: collapse; }
.leads-table th { color: var(--accent); text-align: left; padding: 8px; font-size: 0.8em; border-bottom: 1px solid rgba(255,255,255,0.1); }
.leads-table td { padding: 8px; font-size: 0.85em; border-bottom: 1px solid rgba(255,255,255,0.05); }
.badge { display: inline-block; padding: 2px 8px; border-radius: 8px; font-size: 0.7em; background: rgba(201,162,39,0.2); color: var(--accent); }
.loading { text-align: center; color: var(--muted); padding: 40px; }
.bar { height: 6px; background: rgba(255,255,255,0.1); border-radius: 3px; margin-top: 8px; overflow: hidden; }
.bar-fill { height: 100%; background: var(--accent); border-radius: 3px; transition: width 0.5s ease; }
.section { margin-bottom: 20px; }
</style>
</head>
<body>
<h1>⚖️ SOLIS Dashboard</h1>
<div id="stats" class="grid"><div class="loading">Загрузка...</div></div>
<div class="section">
    <h2>📊 Последние лиды</h2>
    <div id="leads" class="loading">Загрузка...</div>
</div>
<div class="section">
    <h2>🧪 A/B Тесты</h2>
    <div id="ab" class="loading">Загрузка...</div>
</div>
<script>
const tg = window.Telegram?.WebApp;
if (tg) { tg.ready(); tg.expand(); }

async function load() {
    try {
        const [stats, leads, ab] = await Promise.all([
            fetch('/api/stats').then(r => r.json()),
            fetch('/api/leads?limit=10').then(r => r.json()),
            fetch('/api/ab_tests').then(r => r.json()),
        ]);

        // Stats cards
        document.getElementById('stats').innerHTML = `
            <div class="card"><div class="label">Пользователи</div><div class="value">${stats.total_users}</div><div class="sub">Активных: ${stats.active_7d}</div></div>
            <div class="card"><div class="label">Лиды</div><div class="value">${stats.total_leads}</div></div>
            <div class="card"><div class="label">Retention</div><div class="value">${stats.retention_pct}%</div>
                <div class="bar"><div class="bar-fill" style="width:${stats.retention_pct}%"></div></div>
            </div>
            <div class="card"><div class="label">Рефералы</div><div class="value">${stats.total_referrals}</div></div>
        `;

        // Leads table
        if (Array.isArray(leads) && leads.length) {
            let rows = leads.map(l => `<tr><td>${l.name}</td><td>${l.email}</td><td><span class="badge">${l.guide}</span></td></tr>`).join('');
            document.getElementById('leads').innerHTML = `<table class="leads-table"><thead><tr><th>Имя</th><th>Email</th><th>Гайд</th></tr></thead><tbody>${rows}</tbody></table>`;
        } else {
            document.getElementById('leads').innerHTML = '<div class="loading">Нет данных</div>';
        }

        // A/B tests
        let abHtml = '';
        for (const [id, data] of Object.entries(ab)) {
            const winner = data.winner || '—';
            abHtml += `<div class="card"><div class="label">${id}</div><div class="sub">A: ${data.A_rate}% (n=${data.A_views}) | B: ${data.B_rate}% (n=${data.B_views})</div><div class="sub">Winner: ${winner}</div></div>`;
        }
        document.getElementById('ab').innerHTML = abHtml || '<div class="loading">Нет экспериментов</div>';
    } catch(e) {
        document.getElementById('stats').innerHTML = '<div class="loading">Ошибка загрузки</div>';
    }
}
load();
</script>
</body>
</html>"""


@app.get("/", response_class=HTMLResponse)
async def dashboard():
    """Главная страница Mini App."""
    return DASHBOARD_HTML


@app.get("/health")
async def health():
    return {"status": "ok", "service": "solis_mini_app"}
