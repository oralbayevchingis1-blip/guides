"""Growth Engine Tests — имитация 100 виртуальных пользователей.

Тестируются все 10 модулей плана «Масштабирование и Экспансия»:
1. Referral Milestones
2. A/B Testing
3. Smart Broadcasting (сегментация)
4. CRM Webhook
5. Feedback & NPS
6. i18n (мультиязычность)
7. Waitlist
8. Retention Loop
9. UTM/Partner Tracking
10. Growth Report analytics
"""

import asyncio
import hashlib
import time
from collections import Counter
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio


# ═══════════════════════════════════════════════════════════════════════════
#  HELPERS — генерация 100 виртуальных пользователей
# ═══════════════════════════════════════════════════════════════════════════

VIRTUAL_USERS = [
    {
        "user_id": 100_000 + i,
        "username": f"user_{i}",
        "full_name": f"Test User {i}",
        "lang": ["ru", "kz", "en"][i % 3],
        "guide": ["too", "ip", "mfca", "esop", "it_law", "labor", "taxes"][i % 7],
        "source": ["instagram", "linkedin", "partner_acme_campaign1", "ref_100000",
                    "utm_google_cpc", "direct", "channel_pin"][i % 7],
    }
    for i in range(100)
]


# ═══════════════════════════════════════════════════════════════════════════
#  1. REFERRAL MILESTONES
# ═══════════════════════════════════════════════════════════════════════════


class TestReferralMilestones:
    """Тест системы реферальных milestone-достижений."""

    def test_milestone_at_1(self):
        from src.bot.utils.growth_engine import check_referral_milestone
        ms = check_referral_milestone(1)
        assert ms is not None
        assert ms["reward"] == "first_friend"

    def test_milestone_at_3(self):
        from src.bot.utils.growth_engine import check_referral_milestone
        ms = check_referral_milestone(3)
        assert ms is not None
        assert ms["reward"] == "gold_guide"

    def test_milestone_at_10(self):
        from src.bot.utils.growth_engine import check_referral_milestone
        ms = check_referral_milestone(10)
        assert ms is not None
        assert ms["reward"] == "free_consult"

    def test_no_milestone_at_2(self):
        from src.bot.utils.growth_engine import check_referral_milestone
        assert check_referral_milestone(2) is None

    def test_next_milestone(self):
        from src.bot.utils.growth_engine import get_next_milestone
        ms = get_next_milestone(2)
        assert ms is not None
        assert ms["count"] == 3

    def test_next_milestone_after_max(self):
        from src.bot.utils.growth_engine import get_next_milestone
        assert get_next_milestone(30) is None

    def test_progress_text_html(self):
        from src.bot.utils.growth_engine import referral_progress_text
        text = referral_progress_text(4)
        assert "✅" in text  # 1 и 3 достигнуты
        assert "🔒" in text  # 5, 10, 25 не достигнуты

    def test_100_users_referral_flow(self):
        """Имитация: 100 пользователей вступают в реферальную программу."""
        from src.bot.utils.growth_engine import (
            check_referral_milestone,
            get_next_milestone,
        )
        milestones_hit = Counter()
        for u in VIRTUAL_USERS:
            # Каждый привёл i % 15 друзей
            refs = u["user_id"] % 15
            ms = check_referral_milestone(refs)
            if ms:
                milestones_hit[ms["reward"]] += 1
            nxt = get_next_milestone(refs)
            # Следующий milestone всегда > текущего количества
            if nxt:
                assert nxt["count"] > refs

        # Должны быть достигнуты хотя бы несколько milestones
        assert sum(milestones_hit.values()) > 0
        assert "gold_guide" in milestones_hit  # 3 рефералов -> gold


# ═══════════════════════════════════════════════════════════════════════════
#  2. A/B TESTING
# ═══════════════════════════════════════════════════════════════════════════


class TestABTesting:
    """Тест A/B-тестирования офферов."""

    def test_variant_deterministic(self):
        """Один и тот же user_id всегда получает один и тот же вариант."""
        from src.bot.utils.growth_engine import get_ab_variant
        v1 = get_ab_variant("test_x", 12345)
        v2 = get_ab_variant("test_x", 12345)
        assert v1 == v2

    def test_variant_distribution(self):
        """100 пользователей распределяются примерно 50/50."""
        from src.bot.utils.growth_engine import get_ab_variant
        variants = Counter()
        for u in VIRTUAL_USERS:
            v = get_ab_variant("welcome_test", u["user_id"])
            variants[v] += 1

        assert variants["A"] > 20  # Хотя бы 20% в каждой группе
        assert variants["B"] > 20

    def test_conversion_recording(self):
        from src.bot.utils.growth_engine import (
            get_ab_variant,
            record_ab_conversion,
            get_ab_stats,
        )
        test_id = "conv_test"
        for u in VIRTUAL_USERS[:20]:
            get_ab_variant(test_id, u["user_id"])
        # Половина конвертирует
        for u in VIRTUAL_USERS[:10]:
            record_ab_conversion(test_id, u["user_id"])

        stats = get_ab_stats(test_id)
        total_conv = stats["A_conversions"] + stats["B_conversions"]
        assert total_conv == 10

    def test_winner_determination(self):
        """После достаточного количества данных определяется победитель."""
        from src.bot.utils.growth_engine import (
            _ab_experiments,
            _ab_created,
            get_ab_winner,
        )
        _ab_experiments["win_test"] = {
            "A": 50, "B": 50, "A_conv": 30, "B_conv": 10,
        }
        _ab_created["win_test"] = time.time() - 100000

        winner = get_ab_winner("win_test")
        assert winner == "A"


# ═══════════════════════════════════════════════════════════════════════════
#  3. SMART BROADCAST SEGMENTATION
# ═══════════════════════════════════════════════════════════════════════════


class TestSmartBroadcast:
    """Тест сегментации пользователей для умных рассылок."""

    def test_user_interests(self):
        from src.bot.utils.growth_engine import get_user_interests
        leads = [
            {"user_id": "100001", "guide": "it_law_basics"},
            {"user_id": "100001", "guide": "mfca_registration"},
        ]
        interests = get_user_interests(leads, 100001)
        assert "it" in interests
        assert "aifc" in interests

    def test_segment_users(self):
        from src.bot.utils.growth_engine import segment_users

        # Создаём лиды для каждого виртуального пользователя
        leads = [
            {"user_id": str(u["user_id"]), "guide": u["guide"]}
            for u in VIRTUAL_USERS
        ]
        user_ids = [u["user_id"] for u in VIRTUAL_USERS]

        # Сегмент IT
        it_users = segment_users(leads, user_ids, ["it", "tech"])
        assert len(it_users) > 0
        assert len(it_users) < len(user_ids)  # Не все

        # Сегмент finance
        finance_users = segment_users(leads, user_ids, ["finance"])
        assert len(finance_users) > 0

    def test_100_users_segmented(self):
        """Все 100 пользователей получают хотя бы 1 интерес."""
        from src.bot.utils.growth_engine import get_user_interests

        leads = [
            {"user_id": str(u["user_id"]), "guide": u["guide"]}
            for u in VIRTUAL_USERS
        ]

        users_with_interests = 0
        for u in VIRTUAL_USERS:
            interests = get_user_interests(leads, u["user_id"])
            if interests:
                users_with_interests += 1

        # Все 100 должны иметь хотя бы 1 интерес (все качали гайды)
        assert users_with_interests == 100


# ═══════════════════════════════════════════════════════════════════════════
#  4. CRM WEBHOOK
# ═══════════════════════════════════════════════════════════════════════════


class TestCRMWebhook:
    """Тест отправки HOT-лидов в CRM."""

    @pytest.mark.asyncio
    async def test_webhook_disabled(self):
        """Без URL webhook не отправляется."""
        from src.bot.utils.growth_engine import send_crm_webhook
        result = await send_crm_webhook({"user_id": 1, "name": "Test"})
        assert result is False

    @pytest.mark.asyncio
    async def test_webhook_with_url(self):
        """С URL webhook попытается отправить (mock)."""
        from src.bot.utils import growth_engine
        from src.bot.utils.growth_engine import send_crm_webhook

        original = growth_engine.CRM_WEBHOOK_URL
        growth_engine.CRM_WEBHOOK_URL = "https://hooks.example.com/test"

        with patch("aiohttp.ClientSession") as mock_session:
            mock_resp = AsyncMock()
            mock_resp.status = 200
            mock_resp.text = AsyncMock(return_value="ok")

            mock_ctx = AsyncMock()
            mock_ctx.__aenter__ = AsyncMock(return_value=mock_resp)
            mock_ctx.__aexit__ = AsyncMock(return_value=False)

            mock_post = MagicMock(return_value=mock_ctx)

            mock_sess_inst = AsyncMock()
            mock_sess_inst.post = mock_post
            mock_sess_inst.__aenter__ = AsyncMock(return_value=mock_sess_inst)
            mock_sess_inst.__aexit__ = AsyncMock(return_value=False)

            mock_session.return_value = mock_sess_inst

            result = await send_crm_webhook({
                "user_id": 12345,
                "name": "Hot Lead",
                "score": 95,
                "label": "HOT",
            })

        growth_engine.CRM_WEBHOOK_URL = original

    def test_100_leads_webhook_payload(self):
        """100 лидов формируют корректные payload-данные."""
        from src.bot.utils.growth_engine import CRM_WEBHOOK_URL
        import json

        for u in VIRTUAL_USERS:
            payload = {
                "source": "solis_telegram_bot",
                "lead": {
                    "user_id": u["user_id"],
                    "name": u["full_name"],
                    "score": u["user_id"] % 100,
                },
            }
            # Payload должен быть валидным JSON
            json_str = json.dumps(payload)
            assert json.loads(json_str)["lead"]["user_id"] == u["user_id"]


# ═══════════════════════════════════════════════════════════════════════════
#  5. FEEDBACK & NPS
# ═══════════════════════════════════════════════════════════════════════════


class TestFeedbackNPS:
    """Тест системы сбора отзывов."""

    def test_nps_keyboard(self):
        from src.bot.handlers.feedback import _nps_keyboard
        kb = _nps_keyboard()
        # Должно быть 2 ряда: 5 оценок + кнопка пропуска
        assert len(kb.inline_keyboard) == 2
        assert len(kb.inline_keyboard[0]) == 5  # 1-5

    def test_stars_rendering(self):
        from src.bot.handlers.feedback import _stars
        assert _stars(5) == "⭐⭐⭐⭐⭐"
        assert _stars(1) == "⭐☆☆☆☆"
        assert _stars(0) == "☆☆☆☆☆"

    def test_nps_summary(self):
        from src.bot.handlers.feedback import _nps_scores, get_nps_summary

        # Очищаем
        _nps_scores.clear()

        # 100 пользователей оценивают
        for u in VIRTUAL_USERS:
            score = (u["user_id"] % 5) + 1  # 1-5
            _nps_scores.setdefault(u["user_id"], []).append(score)

        summary = get_nps_summary()
        assert summary["total"] == 100
        assert 1 <= summary["avg"] <= 5
        assert summary["promoters"] >= 0
        assert summary["detractors"] >= 0

    @pytest.mark.asyncio
    async def test_schedule_feedback(self):
        """Планирование NPS-запроса через scheduler."""
        from src.bot.handlers.feedback import schedule_feedback

        scheduler = MagicMock()
        scheduler.add_job = MagicMock()
        bot = AsyncMock()

        schedule_feedback(scheduler, bot, user_id=12345, delay_hours=0.01)
        scheduler.add_job.assert_called_once()


# ═══════════════════════════════════════════════════════════════════════════
#  6. MULTILANGUAGE (i18n)
# ═══════════════════════════════════════════════════════════════════════════


class TestI18n:
    """Тест мультиязычной системы."""

    def test_default_language(self):
        from src.bot.utils.i18n import get_user_lang
        assert get_user_lang(999999) == "ru"

    def test_set_language(self):
        from src.bot.utils.i18n import get_user_lang, set_user_lang
        set_user_lang(888, "kz")
        assert get_user_lang(888) == "kz"

    def test_translation(self):
        from src.bot.utils.i18n import t
        ru = t("welcome_subscribed", "ru")
        kz = t("welcome_subscribed", "kz")
        en = t("welcome_subscribed", "en")
        assert "подписан" in ru.lower()
        assert "жазылд" in kz.lower()
        assert "subscribed" in en.lower()

    def test_detect_language_ru(self):
        from src.bot.utils.i18n import detect_language
        assert detect_language("Привет, как дела?") == "ru"

    def test_detect_language_kz(self):
        from src.bot.utils.i18n import detect_language
        assert detect_language("Сәлеметсіз бе, қалай сіз?") == "kz"

    def test_detect_language_en(self):
        from src.bot.utils.i18n import detect_language
        assert detect_language("Hello, how are you?") == "en"

    def test_translation_with_format(self):
        from src.bot.utils.i18n import t
        text = t("rate_limit", "en", limit=10)
        assert "10" in text

    def test_100_users_language_distribution(self):
        """100 пользователей распределяются по 3 языкам."""
        from src.bot.utils.i18n import set_user_lang, get_all_user_langs, _user_languages

        # Сброс
        _user_languages.clear()

        for u in VIRTUAL_USERS:
            set_user_lang(u["user_id"], u["lang"])

        stats = get_all_user_langs()
        assert stats["ru"] > 0
        assert stats["kz"] > 0
        assert stats["en"] > 0
        assert sum(stats.values()) == 100


# ═══════════════════════════════════════════════════════════════════════════
#  7. WAITLIST
# ═══════════════════════════════════════════════════════════════════════════


class TestWaitlist:
    """Тест системы списков ожидания."""

    def test_coming_soon_detection(self):
        from src.bot.utils.waitlist import get_coming_soon
        data_room = [
            {"title": "NFT Legal Review", "status": "Coming Soon", "id": "nft"},
            {"title": "Active Service", "status": "Active", "id": "active"},
            {"title": "Crypto Compliance", "status": "скоро", "id": "crypto"},
        ]
        coming = get_coming_soon(data_room)
        assert len(coming) == 2
        assert any(c["id"] == "nft" for c in coming)
        assert any(c["id"] == "crypto" for c in coming)

    def test_add_to_waitlist(self):
        from src.bot.utils.waitlist import add_to_waitlist, get_waitlist_count, _waitlists
        _waitlists.clear()

        assert add_to_waitlist("svc_1", 100) is True
        assert add_to_waitlist("svc_1", 100) is False  # Дубликат
        assert add_to_waitlist("svc_1", 200) is True
        assert get_waitlist_count("svc_1") == 2

    def test_100_users_join_waitlist(self):
        """100 пользователей записываются в 3 waitlist."""
        from src.bot.utils.waitlist import (
            _waitlists,
            add_to_waitlist,
            get_all_waitlists,
        )
        _waitlists.clear()

        services = ["nft_review", "crypto_compliance", "ai_law"]
        for u in VIRTUAL_USERS:
            svc = services[u["user_id"] % len(services)]
            add_to_waitlist(svc, u["user_id"])

        wl = get_all_waitlists()
        assert len(wl) == 3
        total_subscribers = sum(wl.values())
        assert total_subscribers == 100

    @pytest.mark.asyncio
    async def test_notify_waitlist(self):
        from src.bot.utils.waitlist import (
            _waitlists,
            add_to_waitlist,
            notify_waitlist_release,
        )
        _waitlists.clear()

        for uid in [1, 2, 3]:
            add_to_waitlist("test_svc", uid)

        mock_bot = AsyncMock()
        mock_bot.send_message = AsyncMock()

        result = await notify_waitlist_release(mock_bot, "test_svc", title="New Service")
        assert result["total"] == 3
        assert result["sent"] == 3


# ═══════════════════════════════════════════════════════════════════════════
#  8. RETENTION LOOP
# ═══════════════════════════════════════════════════════════════════════════


class TestRetentionLoop:
    """Тест возвращения спящих пользователей."""

    @pytest.mark.asyncio
    async def test_generate_reengage_message(self):
        from src.bot.utils.retention import _generate_reengage_message

        # Без AI и google — fallback
        msg = await _generate_reengage_message(12345, "Test User")
        assert msg is not None
        assert "Давно не виделись" in msg or "Test User" in msg

    @pytest.mark.asyncio
    async def test_reengage_with_interests(self):
        """С AI mock генерирует персонализированное сообщение."""
        from src.bot.utils.retention import _generate_reengage_message

        mock_google = AsyncMock()
        mock_google.get_recent_leads = AsyncMock(return_value=[
            {"user_id": "100001", "guide": "it_law"},
        ])

        with patch("src.bot.utils.ai_client.ask_marketing", new_callable=AsyncMock) as mock_ai:
            mock_ai.return_value = "Появились обновления по IT-праву!"
            msg = await _generate_reengage_message(100001, "Alice", mock_google)
            assert msg is not None
            assert "Alice" in msg or "обновлен" in msg.lower() or "Давно" in msg

    def test_sleep_threshold(self):
        from src.bot.utils.retention import SLEEP_THRESHOLD_DAYS
        assert SLEEP_THRESHOLD_DAYS == 14


# ═══════════════════════════════════════════════════════════════════════════
#  9. UTM / PARTNER TRACKING
# ═══════════════════════════════════════════════════════════════════════════


class TestUTMPartnerTracking:
    """Тест парсинга UTM-меток и партнёрских ссылок."""

    def test_direct(self):
        from src.bot.utils.growth_engine import parse_utm_source
        result = parse_utm_source("")
        assert result["type"] == "direct"

    def test_referral(self):
        from src.bot.utils.growth_engine import parse_utm_source
        result = parse_utm_source("ref_12345")
        assert result["type"] == "referral"
        assert result["referrer_id"] == "12345"

    def test_partner(self):
        from src.bot.utils.growth_engine import parse_utm_source
        result = parse_utm_source("partner_acme_campaign1")
        assert result["type"] == "partner"
        assert result["partner_id"] == "acme"
        assert result["campaign"] == "campaign1"

    def test_utm(self):
        from src.bot.utils.growth_engine import parse_utm_source
        result = parse_utm_source("utm_google_cpc")
        assert result["type"] == "utm"
        assert result["source"] == "google"
        assert result["campaign"] == "cpc"

    def test_organic(self):
        from src.bot.utils.growth_engine import parse_utm_source
        result = parse_utm_source("instagram")
        assert result["type"] == "organic"
        assert result["source"] == "instagram"

    def test_100_users_source_parsing(self):
        """100 пользователей с разными источниками трафика."""
        from src.bot.utils.growth_engine import parse_utm_source

        type_counts = Counter()
        for u in VIRTUAL_USERS:
            result = parse_utm_source(u["source"])
            type_counts[result["type"]] += 1
            # Каждый результат валиден
            assert result["type"] in ("direct", "referral", "partner", "utm", "organic")
            assert "source" in result

        # Должны быть разные типы
        assert len(type_counts) >= 3


# ═══════════════════════════════════════════════════════════════════════════
#  10. GROWTH REPORT ANALYTICS
# ═══════════════════════════════════════════════════════════════════════════


class TestGrowthReport:
    """Тест аналитического Growth Report."""

    def test_ab_stats_format(self):
        from src.bot.utils.growth_engine import get_ab_stats
        stats = get_ab_stats("nonexistent")
        assert "test_id" in stats
        assert "A_views" in stats
        assert "winner" in stats

    def test_nps_summary_empty(self):
        from src.bot.handlers.feedback import _nps_scores, get_nps_summary
        _nps_scores.clear()
        summary = get_nps_summary()
        assert summary["total"] == 0
        assert summary["nps"] == 0

    def test_waitlist_summary(self):
        from src.bot.utils.waitlist import _waitlists, get_all_waitlists
        _waitlists.clear()
        _waitlists["svc_a"] = [1, 2, 3]
        _waitlists["svc_b"] = [4, 5]

        wl = get_all_waitlists()
        assert wl == {"svc_a": 3, "svc_b": 2}


# ═══════════════════════════════════════════════════════════════════════════
#  INTEGRATION: 100-USER SIMULATION
# ═══════════════════════════════════════════════════════════════════════════


class TestFullSimulation100Users:
    """Интеграционный тест: полный цикл 100 виртуальных пользователей."""

    def test_full_funnel_simulation(self):
        """Имитация полной воронки для 100 пользователей."""
        from src.bot.utils.growth_engine import (
            check_referral_milestone,
            get_ab_variant,
            get_user_interests,
            parse_utm_source,
            record_ab_conversion,
            segment_users,
        )
        from src.bot.utils.i18n import detect_language, set_user_lang, _user_languages
        from src.bot.utils.waitlist import _waitlists, add_to_waitlist
        from src.bot.handlers.feedback import _nps_scores

        # Сброс состояний
        _user_languages.clear()
        _waitlists.clear()
        _nps_scores.clear()

        leads = []
        conversions = 0
        referral_rewards = 0

        for u in VIRTUAL_USERS:
            uid = u["user_id"]

            # 1. Парсим источник трафика
            utm = parse_utm_source(u["source"])
            assert utm["type"] in ("direct", "referral", "partner", "utm", "organic")

            # 2. Определяем язык
            set_user_lang(uid, u["lang"])

            # 3. A/B тест приветствия
            variant = get_ab_variant("welcome_v2", uid)
            assert variant in ("A", "B")

            # 4. Пользователь скачивает гайд
            leads.append({
                "user_id": str(uid),
                "guide": u["guide"],
                "name": u["full_name"],
            })

            # 5. 60% конвертируются (оставляют email)
            if uid % 5 < 3:
                record_ab_conversion("welcome_v2", uid)
                conversions += 1

            # 6. Рефералы: каждый 5-й «приводит» 3 друзей
            if uid % 5 == 0:
                ms = check_referral_milestone(3)
                if ms:
                    referral_rewards += 1

            # 7. Waitlist: каждый 3-й записывается
            if uid % 3 == 0:
                add_to_waitlist("upcoming_service", uid)

            # 8. NPS: все оценивают
            score = (uid % 5) + 1
            _nps_scores.setdefault(uid, []).append(score)

        # Проверки
        assert len(leads) == 100
        assert conversions > 0

        # Сегментация работает
        user_ids = [u["user_id"] for u in VIRTUAL_USERS]
        it_segment = segment_users(leads, user_ids, ["it", "tech"])
        assert len(it_segment) > 0

        # NPS собран
        from src.bot.handlers.feedback import get_nps_summary
        nps = get_nps_summary()
        assert nps["total"] == 100

        # Waitlist заполнен
        from src.bot.utils.waitlist import get_all_waitlists
        wl = get_all_waitlists()
        assert wl.get("upcoming_service", 0) > 0

    def test_language_distribution_realistic(self):
        """Языковое распределение близко к реальному."""
        from src.bot.utils.i18n import detect_language

        texts = {
            "ru": "Добрый день, мне нужна консультация по трудовому праву",
            "kz": "Сәлеметсіз бе, маған кеңес қажет",
            "en": "Hello, I need legal advice regarding my company",
        }

        for lang, text in texts.items():
            detected = detect_language(text)
            assert detected == lang, f"Expected {lang}, got {detected} for: {text}"

    def test_partner_tracking_unique_ids(self):
        """Партнёрские ID уникально распознаются."""
        from src.bot.utils.growth_engine import parse_utm_source

        partner_ids = set()
        for u in VIRTUAL_USERS:
            utm = parse_utm_source(u["source"])
            if utm["type"] == "partner":
                partner_ids.add(utm["partner_id"])

        # Должен быть хотя бы 1 уникальный партнёр
        assert len(partner_ids) >= 1
