"""Growth Hacker Report — еженедельный автономный AI-отчёт.

Раз в неделю AI анализирует:
- Рост пользователей и лидов
- Эффективность реферальной программы
- A/B тесты и конверсии
- Слабые места воронки
- NPS и удовлетворённость

Генерирует рекомендации по улучшению.
"""

import logging
from datetime import datetime, timedelta, timezone

from aiogram import Bot
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup

from src.config import settings

logger = logging.getLogger(__name__)


async def send_growth_report(
    bot: Bot,
    google=None,
    cache=None,
) -> None:
    """Генерирует и отправляет еженедельный Growth Report админу."""
    try:
        now = datetime.now(timezone.utc)
        week_ago = now - timedelta(days=7)
        today_str = now.strftime("%d.%m.%Y")

        # 1. Собираем данные
        total_users = 0
        active_7d = 0
        try:
            from src.database.models import async_session, User
            from sqlalchemy import select, func as sa_func

            async with async_session() as session:
                total_users = (await session.execute(
                    select(sa_func.count(User.id))
                )).scalar() or 0
                active_7d = (await session.execute(
                    select(sa_func.count(User.id)).where(
                        User.last_activity >= week_ago
                    )
                )).scalar() or 0
        except Exception:
            pass

        # 2. Лиды
        leads = []
        if google:
            try:
                leads = await google.get_recent_leads(limit=500)
            except Exception:
                pass

        week_leads = [
            l for l in leads
            if l.get("timestamp", "")[:10] >= week_ago.strftime("%Y-%m-%d")
            or l.get("timestamp", "")[:10] >= week_ago.strftime("%d.%m.%Y")
        ]

        # 3. A/B stats
        ab_report = ""
        try:
            from src.bot.utils.growth_engine import get_ab_stats
            stats = get_ab_stats("email_cta")
            if stats.get("A_views", 0) > 0:
                ab_report = (
                    f"A/B «email_cta»: A={stats['A_rate']}% vs B={stats['B_rate']}%"
                    f" (winner: {stats.get('winner', '?')})"
                )
        except Exception:
            pass

        # 4. NPS
        nps_report = ""
        try:
            from src.bot.handlers.feedback import get_nps_summary
            nps = get_nps_summary()
            if nps["total"] > 0:
                nps_report = f"NPS: {nps['nps']} (avg: {nps['avg']}/5, n={nps['total']})"
        except Exception:
            pass

        # 5. Referral stats
        ref_report = ""
        try:
            from src.database.models import Referral
            from sqlalchemy import select, func as sa_func

            async with async_session() as session:
                ref_total = (await session.execute(
                    select(sa_func.count(Referral.id))
                )).scalar() or 0
                ref_week = (await session.execute(
                    select(sa_func.count(Referral.id)).where(
                        Referral.created_at >= week_ago
                    )
                )).scalar() or 0
            ref_report = f"Рефералов за неделю: {ref_week} (всего: {ref_total})"
        except Exception:
            pass

        # 6. Waitlist
        waitlist_report = ""
        try:
            from src.bot.utils.waitlist import get_all_waitlists
            wl = get_all_waitlists()
            if wl:
                waitlist_report = "Waitlists: " + ", ".join(
                    f"{k}: {v}" for k, v in wl.items()
                )
        except Exception:
            pass

        # 7. AI анализ и рекомендации
        ai_analysis = ""
        try:
            from src.bot.utils.ai_client import ask_marketing

            data_prompt = (
                f"Данные за неделю ({today_str}):\n"
                f"- Пользователей всего: {total_users}\n"
                f"- Активных за 7 дней: {active_7d}\n"
                f"- Новых лидов: {len(week_leads)}\n"
            )
            if ab_report:
                data_prompt += f"- {ab_report}\n"
            if nps_report:
                data_prompt += f"- {nps_report}\n"
            if ref_report:
                data_prompt += f"- {ref_report}\n"

            data_prompt += (
                "\nНапиши Growth Report (3-5 предложений):\n"
                "1. Общая оценка роста (% если возможно)\n"
                "2. Что работает хорошо\n"
                "3. Слабое место воронки\n"
                "4. Конкретная рекомендация по улучшению\n"
                "5. Предложи новый текст/оффер если нужно"
            )

            ai_analysis = await ask_marketing(
                prompt=data_prompt,
                max_tokens=1024,
                temperature=0.5,
            )
        except Exception as e:
            ai_analysis = f"(AI-анализ недоступен: {e})"

        # 8. Формируем отчёт
        from src.bot.utils.visual import progress_bar, stat_card

        report = (
            f"📈 <b>Growth Report — неделя {today_str}</b>\n"
            f"{'═' * 30}\n\n"
        )

        report += stat_card("Аудитория", {
            "Всего": total_users,
            "Активных (7д)": active_7d,
            "Retention": f"{round(active_7d / total_users * 100)}%" if total_users else "0%",
        }, emoji="👥") + "\n\n"

        report += (
            f"🔥 <b>Лиды:</b>\n"
            f"<code>{progress_bar(len(week_leads), max(20, len(week_leads) + 10), label='За неделю')}</code>\n\n"
        )

        if ref_report:
            report += f"🤝 {ref_report}\n"
        if ab_report:
            report += f"🧪 {ab_report}\n"
        if nps_report:
            report += f"⭐ {nps_report}\n"
        if waitlist_report:
            report += f"📋 {waitlist_report}\n"

        report += f"\n{'─' * 30}\n"
        report += f"🧠 <b>AI-анализ:</b>\n{ai_analysis}\n"

        # Кнопки
        keyboard = InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(
                    text="📊 Открыть CRM",
                    url=f"https://docs.google.com/spreadsheets/d/{settings.GOOGLE_SPREADSHEET_ID}/edit",
                )],
                [InlineKeyboardButton(text="✅ Принято", callback_data="digest_ack")],
            ]
        )

        try:
            await bot.send_message(chat_id=settings.ADMIN_ID, text=report, reply_markup=keyboard)
        except Exception:
            await bot.send_message(
                chat_id=settings.ADMIN_ID, text=report, reply_markup=keyboard, parse_mode=None,
            )

        logger.info("Growth report sent to admin")

    except Exception as e:
        logger.error("Growth report error: %s", e)
