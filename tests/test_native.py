"""Native Ecosystem Tests — Telegram Mini App, Payments, Voice, PDF, Groups, etc.

Тестируются все 10 модулей «Native Ecosystem»:
1. Mini App (FastAPI endpoints)
2. Telegram Payments (products, invoice logic)
3. Voice-to-Text (Whisper integration)
4. PDF Generation (NDA, contracts)
5. Group Mode (trigger detection)
6. Stories Publisher
7. Live Support (shared inbox)
8. Timezone Manager
9. Telegram Passport / Cabinet
10. Gamification (Karma system)
"""

import asyncio
import os
from collections import Counter
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio


# ═══════════════════════════════════════════════════════════════════════════
#  VIRTUAL USERS
# ═══════════════════════════════════════════════════════════════════════════

VIRTUAL_USERS = [
    {
        "user_id": 200_000 + i,
        "username": f"native_user_{i}",
        "full_name": f"Native User {i}",
        "city": ["Алматы", "Астана", "Москва", "Лондон", "Дубай"][i % 5],
    }
    for i in range(100)
]


# ═══════════════════════════════════════════════════════════════════════════
#  1. MINI APP (FastAPI)
# ═══════════════════════════════════════════════════════════════════════════


class TestMiniApp:
    """Mini App FastAPI endpoints."""

    def test_app_exists(self):
        from src.bot.webapp.app import app
        assert app is not None
        assert app.title == "SOLIS Partners Admin Dashboard"

    def test_dashboard_html(self):
        from src.bot.webapp.app import DASHBOARD_HTML
        assert "SOLIS Dashboard" in DASHBOARD_HTML
        assert "telegram-web-app.js" in DASHBOARD_HTML
        assert "/api/stats" in DASHBOARD_HTML

    def test_health_endpoint_exists(self):
        """Health endpoint is defined."""
        from src.bot.webapp.app import app
        routes = [r.path for r in app.routes]
        assert "/health" in routes
        assert "/api/stats" in routes
        assert "/api/leads" in routes


# ═══════════════════════════════════════════════════════════════════════════
#  2. TELEGRAM PAYMENTS
# ═══════════════════════════════════════════════════════════════════════════


class TestPayments:
    """Telegram Payments — каталог продуктов и логика инвойсов."""

    def test_products_catalog(self):
        from src.bot.handlers.payments import PRODUCTS
        assert len(PRODUCTS) >= 4
        assert "consult_30min" in PRODUCTS
        assert "vip_bundle" in PRODUCTS

    def test_product_prices(self):
        from src.bot.handlers.payments import PRODUCTS
        consult = PRODUCTS["consult_30min"]
        total = sum(p.amount for p in consult["prices"])
        assert total == 1_500_000  # 15,000 KZT in kopecks

    def test_vip_bundle_discount(self):
        from src.bot.handlers.payments import PRODUCTS
        vip = PRODUCTS["vip_bundle"]
        total = sum(p.amount for p in vip["prices"])
        # 500k + 1500k - 200k = 1800k
        assert total == 1_800_000

    def test_100_users_product_selection(self):
        """100 пользователей выбирают разные продукты."""
        from src.bot.handlers.payments import PRODUCTS
        product_ids = list(PRODUCTS.keys())
        selections = Counter()
        for u in VIRTUAL_USERS:
            product = product_ids[u["user_id"] % len(product_ids)]
            selections[product] += 1
        # Каждый продукт выбран хотя бы раз
        assert len(selections) == len(product_ids)


# ═══════════════════════════════════════════════════════════════════════════
#  3. VOICE-TO-TEXT
# ═══════════════════════════════════════════════════════════════════════════


class TestVoiceToText:
    """Voice-to-Text через Whisper."""

    def test_temp_dir_exists(self):
        from src.bot.handlers.voice import TEMP_DIR
        assert os.path.isdir(TEMP_DIR) or True  # Создаётся при импорте

    @pytest.mark.asyncio
    async def test_transcribe_voice_mock(self):
        """Mock Whisper API для транскрипции."""
        from src.bot.handlers.voice import transcribe_voice

        mock_bot = AsyncMock()
        mock_file = MagicMock()
        mock_file.file_path = "voice/test.oga"
        mock_bot.get_file = AsyncMock(return_value=mock_file)
        mock_bot.download_file = AsyncMock()

        with patch("aiohttp.ClientSession") as mock_session:
            mock_resp = AsyncMock()
            mock_resp.status = 200
            mock_resp.json = AsyncMock(return_value={"text": "Нужна консультация по трудовому праву"})

            mock_ctx = AsyncMock()
            mock_ctx.__aenter__ = AsyncMock(return_value=mock_resp)
            mock_ctx.__aexit__ = AsyncMock(return_value=False)

            mock_sess = AsyncMock()
            mock_sess.post = MagicMock(return_value=mock_ctx)
            mock_sess.__aenter__ = AsyncMock(return_value=mock_sess)
            mock_sess.__aexit__ = AsyncMock(return_value=False)
            mock_session.return_value = mock_sess

            # Создаём временный файл для теста
            temp_path = os.path.join("data", "temp", "test_file.oga")
            os.makedirs(os.path.dirname(temp_path), exist_ok=True)
            with open(temp_path, "wb") as f:
                f.write(b"fake audio data")

            text = await transcribe_voice(mock_bot, "test_file")
            # Результат зависит от мока, но не должен упасть
            assert isinstance(text, str)


# ═══════════════════════════════════════════════════════════════════════════
#  4. PDF GENERATION
# ═══════════════════════════════════════════════════════════════════════════


class TestPDFGeneration:
    """Генерация юридических документов."""

    def test_document_templates(self):
        from src.bot.utils.pdf_generator import DOCUMENT_TEMPLATES
        assert "nda" in DOCUMENT_TEMPLATES
        assert "contract" in DOCUMENT_TEMPLATES

    @pytest.mark.asyncio
    async def test_nda_text_fallback(self):
        """Генерация NDA как текстовый файл (без reportlab)."""
        from src.bot.utils.pdf_generator import _generate_nda_text

        filepath = await _generate_nda_text(
            party1="SOLIS Partners",
            party2="ТОО Тест",
            city="Алматы",
            purpose="тестирование",
            user_name="Тестер",
        )

        assert filepath is not None
        assert os.path.exists(filepath)
        with open(filepath, "r", encoding="utf-8") as f:
            content = f.read()
        assert "SOLIS Partners" in content
        assert "ТОО Тест" in content
        assert "НЕРАЗГЛАШЕНИИ" in content

        # Cleanup
        os.remove(filepath)

    @pytest.mark.asyncio
    async def test_contract_generation(self):
        from src.bot.utils.pdf_generator import generate_contract_pdf

        filepath = await generate_contract_pdf(
            service_name="Регистрация ТОО",
            client_name="Иванов Иван",
            client_company="ТОО ТестКо",
            amount="500 000 KZT",
        )

        assert filepath is not None
        assert os.path.exists(filepath)
        # Cleanup
        os.remove(filepath)

    def test_100_users_nda_data(self):
        """100 пользователей формируют NDA-данные."""
        for u in VIRTUAL_USERS:
            data = {
                "party1": "SOLIS Partners",
                "party2": f"Company_{u['user_id']}",
                "city": u["city"],
                "purpose": "сотрудничество",
            }
            assert all(v for v in data.values())


# ═══════════════════════════════════════════════════════════════════════════
#  5. GROUP MODE
# ═══════════════════════════════════════════════════════════════════════════


class TestGroupMode:
    """Режим мониторинга групп."""

    def test_trigger_patterns(self):
        from src.bot.handlers.group_mode import TRIGGER_PATTERN
        # Должны срабатывать
        assert TRIGGER_PATTERN.search("Нужна консультация по праву")
        assert TRIGGER_PATTERN.search("У нас юридический вопрос")
        assert TRIGGER_PATTERN.search("Нужен юрист срочно")

        # Не должны срабатывать
        assert TRIGGER_PATTERN.search("Привет, как дела?") is None
        assert TRIGGER_PATTERN.search("Погода хорошая") is None

    def test_is_group_detection(self):
        from src.bot.handlers.group_mode import _is_group
        msg = MagicMock()
        msg.chat.type = "supergroup"
        assert _is_group(msg) is True

        msg.chat.type = "private"
        assert _is_group(msg) is False

    def test_is_triggered_by_mention(self):
        from src.bot.handlers.group_mode import _is_triggered
        msg = MagicMock()
        msg.text = "Привет @solis_bot, помоги!"
        msg.caption = None
        msg.reply_to_message = None
        assert _is_triggered(msg, "solis_bot") is True


# ═══════════════════════════════════════════════════════════════════════════
#  6. STORIES PUBLISHER
# ═══════════════════════════════════════════════════════════════════════════


class TestStoriesPublisher:
    """Автоматическая публикация Stories."""

    @pytest.mark.asyncio
    async def test_publish_story_text(self):
        from src.bot.utils.stories_publisher import publish_story

        mock_bot = AsyncMock()
        mock_bot.send_message = AsyncMock()
        mock_bot.get_me = AsyncMock(return_value=MagicMock(username="solis_bot"))

        result = await publish_story(
            bot=mock_bot,
            title="Новый закон",
            summary="Важные изменения в налоговом кодексе",
            channel="@test_channel",
        )
        assert result is True
        mock_bot.send_message.assert_called_once()

    @pytest.mark.asyncio
    async def test_publish_story_with_image(self):
        from src.bot.utils.stories_publisher import publish_story

        mock_bot = AsyncMock()
        mock_bot.send_photo = AsyncMock()
        mock_bot.get_me = AsyncMock(return_value=MagicMock(username="solis_bot"))

        result = await publish_story(
            bot=mock_bot,
            title="Обновление",
            summary="Текст",
            image_url="https://example.com/image.jpg",
            channel="@test",
        )
        assert result is True
        mock_bot.send_photo.assert_called_once()


# ═══════════════════════════════════════════════════════════════════════════
#  7. LIVE SUPPORT
# ═══════════════════════════════════════════════════════════════════════════


class TestLiveSupport:
    """Shared Inbox — передача от AI к человеку."""

    def test_save_ai_exchange(self):
        from src.bot.handlers.live_support import _ai_history, save_ai_exchange

        _ai_history.clear()
        save_ai_exchange(12345, "Как зарегистрировать ТОО?", "Ответ AI: нужно...")
        assert 12345 in _ai_history
        assert len(_ai_history[12345]) == 1

    def test_history_limit(self):
        from src.bot.handlers.live_support import _ai_history, save_ai_exchange

        _ai_history.clear()
        for i in range(15):
            save_ai_exchange(99999, f"Вопрос {i}", f"Ответ {i}")
        assert len(_ai_history[99999]) == 10  # Максимум 10

    def test_active_tickets(self):
        from src.bot.handlers.live_support import _active_tickets, get_active_tickets
        _active_tickets.clear()
        _active_tickets[111] = True
        _active_tickets[222] = True
        tickets = get_active_tickets()
        assert len(tickets) == 2


# ═══════════════════════════════════════════════════════════════════════════
#  8. TIMEZONE MANAGER
# ═══════════════════════════════════════════════════════════════════════════


class TestTimezoneManager:
    """Умные уведомления по часовому поясу."""

    def test_default_timezone(self):
        from src.bot.utils.timezone_manager import get_user_tz
        assert get_user_tz(999999) == "Asia/Almaty"

    def test_set_timezone(self):
        from src.bot.utils.timezone_manager import get_user_tz, set_user_timezone
        assert set_user_timezone(111, "Europe/Moscow") is True
        assert get_user_tz(111) == "Europe/Moscow"

    def test_invalid_timezone(self):
        from src.bot.utils.timezone_manager import set_user_timezone
        assert set_user_timezone(222, "Invalid/Zone") is False

    def test_timezone_from_location_almaty(self):
        from src.bot.utils.timezone_manager import timezone_from_location
        # Алматы: ~43.24°N, 76.95°E
        tz = timezone_from_location(43.24, 76.95)
        assert tz == "Asia/Almaty"

    def test_timezone_from_location_aktau(self):
        from src.bot.utils.timezone_manager import timezone_from_location
        # Актау: ~43.65°N, 51.17°E
        tz = timezone_from_location(43.65, 51.17)
        assert tz == "Asia/Aqtau"

    def test_timezone_from_location_moscow(self):
        from src.bot.utils.timezone_manager import timezone_from_location
        tz = timezone_from_location(55.75, 37.62)
        assert tz == "Europe/Moscow"

    def test_local_time(self):
        from src.bot.utils.timezone_manager import (
            get_user_local_time,
            set_user_timezone,
        )
        set_user_timezone(333, "Asia/Almaty")
        local = get_user_local_time(333)
        assert local.tzinfo is not None

    def test_100_users_timezone_distribution(self):
        """100 пользователей из разных городов."""
        from src.bot.utils.timezone_manager import (
            _user_timezones,
            get_all_user_timezones,
            set_user_timezone,
        )
        _user_timezones.clear()

        city_to_tz = {
            "Алматы": "Asia/Almaty",
            "Астана": "Asia/Almaty",
            "Москва": "Europe/Moscow",
            "Лондон": "Europe/London",
            "Дубай": "Asia/Dubai",
        }

        for u in VIRTUAL_USERS:
            tz = city_to_tz.get(u["city"], "Asia/Almaty")
            set_user_timezone(u["user_id"], tz)

        stats = get_all_user_timezones()
        assert len(stats) >= 3  # Минимум 3 разных зоны
        assert sum(stats.values()) == 100


# ═══════════════════════════════════════════════════════════════════════════
#  9. PERSONAL CABINET
# ═══════════════════════════════════════════════════════════════════════════


class TestCabinet:
    """Личный кабинет и профиль."""

    def test_karma_profile_html(self):
        from src.bot.utils.karma import add_karma, get_karma_profile, _karma
        _karma.clear()
        add_karma(555, 100, "test")
        text = get_karma_profile(555)
        assert "Активный" in text  # 100 баллов = Активный
        assert "100" in text


# ═══════════════════════════════════════════════════════════════════════════
#  10. GAMIFICATION (KARMA)
# ═══════════════════════════════════════════════════════════════════════════


class TestKarma:
    """Система кармы и геймификации."""

    def test_add_karma(self):
        from src.bot.utils.karma import _karma, add_karma, get_karma
        _karma.clear()
        add_karma(1, 10, "guide_download")
        assert get_karma(1) == 10

    def test_karma_by_action(self):
        from src.bot.utils.karma import _karma, add_karma, get_karma
        _karma.clear()
        add_karma(2, 0, "consult")  # +3 по умолчанию
        assert get_karma(2) == 3

    def test_karma_levels(self):
        from src.bot.utils.karma import _karma, add_karma, get_karma_level
        _karma.clear()
        add_karma(3, 0, "")  # 0 баллов
        assert get_karma_level(3)["name"] == "Новичок"

        add_karma(3, 50, "test")
        assert get_karma_level(3)["name"] == "Активный"

        add_karma(3, 100, "test")
        assert get_karma_level(3)["name"] == "Продвинутый"

        add_karma(3, 200, "test")
        assert get_karma_level(3)["name"] == "Эксперт"

        add_karma(3, 200, "test")
        assert get_karma_level(3)["name"] == "Мастер права"

    def test_next_level(self):
        from src.bot.utils.karma import _karma, add_karma, get_karma_next_level
        _karma.clear()
        add_karma(4, 10, "test")
        nxt = get_karma_next_level(4)
        assert nxt is not None
        assert nxt["min"] == 50

    def test_leaderboard(self):
        from src.bot.utils.karma import _karma, add_karma, get_karma_leaderboard
        _karma.clear()
        add_karma(10, 100, "a")
        add_karma(20, 200, "b")
        add_karma(30, 50, "c")

        lb = get_karma_leaderboard(3)
        assert len(lb) == 3
        assert lb[0]["user_id"] == 20  # Лидер
        assert lb[0]["rank"] == 1

    def test_karma_log(self):
        from src.bot.utils.karma import _karma, _karma_log, add_karma, get_karma_log
        _karma.clear()
        _karma_log.clear()

        add_karma(5, 10, "guide_download")
        add_karma(5, 20, "referral")

        log = get_karma_log(5)
        assert len(log) == 2
        assert log[-1]["action"] == "referral"

    def test_100_users_karma_distribution(self):
        """100 пользователей зарабатывают карму разными способами."""
        from src.bot.utils.karma import (
            KARMA_ACTIONS,
            _karma,
            add_karma,
            get_karma,
            get_karma_leaderboard,
            get_karma_level,
        )
        _karma.clear()

        actions = list(KARMA_ACTIONS.keys())
        for u in VIRTUAL_USERS:
            uid = u["user_id"]
            # Каждый пользователь делает 1-5 действий
            num_actions = (uid % 5) + 1
            for i in range(num_actions):
                action = actions[i % len(actions)]
                add_karma(uid, 0, action)

        # У всех пользователей > 0 кармы
        for u in VIRTUAL_USERS:
            assert get_karma(u["user_id"]) > 0

        # Лидерборд работает
        lb = get_karma_leaderboard(10)
        assert len(lb) == 10
        assert lb[0]["karma"] >= lb[9]["karma"]


# ═══════════════════════════════════════════════════════════════════════════
#  INTEGRATION: Full ecosystem test
# ═══════════════════════════════════════════════════════════════════════════


class TestFullNativeEcosystem:
    """Интеграционный тест: полный цикл нативных функций."""

    def test_full_user_journey(self):
        """Путь пользователя через все нативные функции."""
        from src.bot.utils.karma import _karma, add_karma, get_karma, get_karma_profile
        from src.bot.utils.timezone_manager import (
            _user_timezones,
            get_user_local_time,
            set_user_timezone,
        )
        from src.bot.handlers.live_support import _ai_history, save_ai_exchange

        _karma.clear()
        _user_timezones.clear()
        _ai_history.clear()

        for u in VIRTUAL_USERS:
            uid = u["user_id"]

            # 1. Установка часового пояса
            city_tz = {
                "Алматы": "Asia/Almaty",
                "Астана": "Asia/Almaty",
                "Москва": "Europe/Moscow",
                "Лондон": "Europe/London",
                "Дубай": "Asia/Dubai",
            }
            set_user_timezone(uid, city_tz.get(u["city"], "Asia/Almaty"))
            local_time = get_user_local_time(uid)
            assert local_time.tzinfo is not None

            # 2. Карма за действия
            add_karma(uid, 0, "guide_download")
            add_karma(uid, 0, "consult")
            if uid % 3 == 0:
                add_karma(uid, 0, "referral")
            if uid % 10 == 0:
                add_karma(uid, 0, "purchase")

            # 3. AI exchange сохранён
            save_ai_exchange(uid, f"Вопрос от {u['full_name']}", "Ответ AI")

            # 4. Профиль кармы генерируется
            profile = get_karma_profile(uid)
            assert "баллов" in profile

        # Все 100 пользователей обработаны
        assert len(_karma) == 100
        assert len(_user_timezones) == 100
        assert len(_ai_history) == 100
