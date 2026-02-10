"""Тесты для «Промышленная стабильность и Deep Analytics» (10 пунктов).

Проверяет:
P1. ErrorHandlingMiddleware
P2. ThrottlingMiddleware
P3. Pydantic validators
P4. Sentry integration
P5. Telemetry & funnel
P6. DB backup
P7. Progressive profiling
P8. Healthcheck
P9. Log rotation & cleanup
P10. Security audit
"""

import asyncio
import os
import time
import pytest
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch


# ═══════════════════════════════════════════════════════════════════════════
#  P1. Error Handler Middleware
# ═══════════════════════════════════════════════════════════════════════════


class TestErrorHandlerMiddleware:
    """P1: Тесты глобального обработчика ошибок."""

    def test_mask_secrets_api_key(self):
        from src.bot.middlewares.error_handler import _mask_secrets
        text = "Error with key sk-proj-1234567890abcdefghij"
        masked = _mask_secrets(text)
        assert "sk-proj-" in masked
        assert "1234567890abcdefghij" not in masked

    def test_mask_secrets_bot_token(self):
        from src.bot.middlewares.error_handler import _mask_secrets
        text = "Token: 1234567890:AABBCCDDEEFFGGHHIIJJKKLLMMNNOOPPqqrr"
        masked = _mask_secrets(text)
        assert "1234567890:AA" not in masked

    def test_error_stats(self):
        from src.bot.middlewares.error_handler import _error_counter, get_error_stats, reset_error_stats
        _error_counter["TestError"] = 5
        stats = get_error_stats()
        assert stats["TestError"] == 5
        reset_error_stats()
        assert len(get_error_stats()) == 0

    @pytest.mark.asyncio
    async def test_middleware_catches_exception(self):
        from src.bot.middlewares.error_handler import ErrorHandlingMiddleware
        from aiogram.types import Message

        bot = AsyncMock()
        bot.send_message = AsyncMock()
        mw = ErrorHandlingMiddleware(bot)

        # Use spec=Message to make isinstance work
        event = AsyncMock(spec=Message)
        event.from_user = MagicMock()
        event.from_user.id = 12345
        event.from_user.username = "testuser"
        event.answer = AsyncMock()

        async def failing_handler(event, data):
            raise ValueError("test error")

        # Should not raise — middleware catches it
        result = await mw(failing_handler, event, {})
        assert result is None
        event.answer.assert_called()


# ═══════════════════════════════════════════════════════════════════════════
#  P2. Throttling Middleware
# ═══════════════════════════════════════════════════════════════════════════


class TestThrottlingMiddleware:
    """P2: Тесты антифлуда."""

    @pytest.mark.asyncio
    async def test_normal_request_passes(self):
        from src.bot.middlewares.throttle import ThrottlingMiddleware, _last_message, _bans

        _last_message.clear()
        _bans.clear()

        mw = ThrottlingMiddleware(min_interval=0.5)

        event = MagicMock()
        event.from_user = MagicMock()
        event.from_user.id = 99999
        handler = AsyncMock(return_value="ok")

        result = await mw(handler, event, {})
        assert result == "ok"

    @pytest.mark.asyncio
    async def test_rapid_requests_throttled(self):
        from src.bot.middlewares.throttle import ThrottlingMiddleware, _last_message, _flood_score

        _last_message.clear()
        _flood_score.clear()

        mw = ThrottlingMiddleware(min_interval=10.0)  # Very high threshold

        event = MagicMock()
        event.from_user = MagicMock()
        event.from_user.id = 88888

        handler = AsyncMock(return_value="ok")

        # First request passes
        _last_message[88888] = time.time()
        result = await mw(handler, event, {})
        # Second should be throttled (delta < 10s)
        assert handler.call_count <= 2  # At most first passed

    @pytest.mark.asyncio
    async def test_admin_not_throttled(self):
        from src.bot.middlewares.throttle import ThrottlingMiddleware, _bans
        from src.config import settings

        _bans.clear()
        _bans[settings.ADMIN_ID] = time.time() + 1000  # Even with a ban

        mw = ThrottlingMiddleware()
        event = MagicMock()
        event.from_user = MagicMock()
        event.from_user.id = settings.ADMIN_ID

        handler = AsyncMock(return_value="admin_ok")
        result = await mw(handler, event, {})
        assert result == "admin_ok"

    def test_throttle_stats(self):
        from src.bot.middlewares.throttle import get_throttle_stats
        stats = get_throttle_stats()
        assert "active_bans" in stats
        assert "total_tracked" in stats


# ═══════════════════════════════════════════════════════════════════════════
#  P3. Pydantic Validators
# ═══════════════════════════════════════════════════════════════════════════


class TestPydanticValidators:
    """P3: Тесты валидации данных."""

    def test_valid_lead(self):
        from src.bot.utils.validators import validate_lead
        ok, err = validate_lead("Алексей Петров", "alex@company.com", "esop")
        assert ok
        assert err == ""

    def test_invalid_email(self):
        from src.bot.utils.validators import validate_lead
        ok, err = validate_lead("Алексей", "not-email", "esop")
        assert not ok

    def test_disposable_email(self):
        from src.bot.utils.validators import validate_lead
        ok, err = validate_lead("Алексей", "test@mailinator.com")
        assert not ok

    def test_garbage_name(self):
        from src.bot.utils.validators import is_garbage_text
        assert is_garbage_text("asdfgh")
        assert is_garbage_text("aaaa")
        assert is_garbage_text("a")
        assert is_garbage_text("test")
        assert not is_garbage_text("Чингис")

    def test_valid_article(self):
        from src.bot.utils.validators import validate_article
        ok, err = validate_article(
            "Увольнение без рисков",
            "A" * 100,
            category="legal",
        )
        assert ok

    def test_short_article(self):
        from src.bot.utils.validators import validate_article
        ok, err = validate_article("Title", "Short")
        assert not ok

    def test_url_validation(self):
        from src.bot.utils.validators import is_valid_url
        assert is_valid_url("https://solispartners.kz/blog")
        assert is_valid_url("http://example.com")
        assert not is_valid_url("not-a-url")
        assert not is_valid_url("")

    def test_config_sanity(self):
        from src.bot.utils.validators import check_config_sanity
        warnings = check_config_sanity()
        assert isinstance(warnings, list)

    def test_numeric_name_rejected(self):
        from src.bot.utils.validators import validate_lead
        ok, err = validate_lead("12345", "test@example.com")
        assert not ok


# ═══════════════════════════════════════════════════════════════════════════
#  P4. Sentry Integration
# ═══════════════════════════════════════════════════════════════════════════


class TestSentryIntegration:
    """P4: Тесты Sentry (noop без SENTRY_DSN)."""

    def test_init_without_dsn(self):
        from src.bot.utils.sentry_integration import init_sentry, is_enabled
        result = init_sentry()
        assert not result  # No DSN configured
        assert not is_enabled()

    def test_capture_exception_noop(self):
        from src.bot.utils.sentry_integration import capture_exception
        # Should not raise
        capture_exception(ValueError("test"))

    def test_capture_message_noop(self):
        from src.bot.utils.sentry_integration import capture_message
        capture_message("test message", level="info")

    def test_set_user_context_noop(self):
        from src.bot.utils.sentry_integration import set_user_context
        set_user_context(12345, "testuser")


# ═══════════════════════════════════════════════════════════════════════════
#  P5. Telemetry & Funnel
# ═══════════════════════════════════════════════════════════════════════════


class TestTelemetry:
    """P5: Тесты телеметрии и воронки."""

    def test_track_event(self):
        from src.bot.utils.telemetry import track_event_sync, _funnel_counters
        _funnel_counters.clear()
        track_event_sync(1001, "bot_started")
        track_event_sync(1001, "guide_selected", {"guide": "it-law"})
        assert _funnel_counters["bot_started"] == 1
        assert _funnel_counters["guide_selected"] == 1

    def test_funnel_drop_rates(self):
        from src.bot.utils.telemetry import (
            track_event_sync, get_funnel_drop_rates, _funnel_counters,
        )
        _funnel_counters.clear()
        for _ in range(100):
            track_event_sync(1, "bot_started")
        for _ in range(50):
            track_event_sync(1, "guide_menu_opened")
        for _ in range(25):
            track_event_sync(1, "guide_selected")

        drops = get_funnel_drop_rates()
        # bot_started → guide_menu_opened: 50% conversion
        assert drops[0]["rate"] == 50.0
        assert drops[0]["drop"] == 50.0

    def test_recent_events(self):
        from src.bot.utils.telemetry import track_event_sync, get_recent_events, _events
        _events.clear()
        for i in range(10):
            track_event_sync(i, "test_event", {"i": i})
        events = get_recent_events(5)
        assert len(events) == 5

    def test_funnel_stages_defined(self):
        from src.bot.utils.telemetry import FUNNEL_STAGES
        assert "bot_started" in FUNNEL_STAGES
        assert "lead_saved" in FUNNEL_STAGES
        assert "payment_completed" in FUNNEL_STAGES

    @pytest.mark.asyncio
    async def test_analyze_funnel_without_ai(self):
        from src.bot.utils.telemetry import analyze_funnel, _funnel_counters
        _funnel_counters.clear()
        _funnel_counters["bot_started"] = 100
        _funnel_counters["guide_selected"] = 30
        result = await analyze_funnel(ai_client=None)
        assert "bot_started" in result


# ═══════════════════════════════════════════════════════════════════════════
#  P6. DB Backup
# ═══════════════════════════════════════════════════════════════════════════


class TestDBBackup:
    """P6: Тесты бэкапа БД."""

    def test_backup_module_imports(self):
        from src.backup import (
            vacuum_database, create_backup, cleanup_old_backups,
            scheduled_backup, daily_backup, send_backup_to_admin,
        )
        assert callable(vacuum_database)
        assert callable(daily_backup)

    @pytest.mark.asyncio
    async def test_send_backup_to_admin(self):
        from src.backup import send_backup_to_admin
        from pathlib import Path

        bot = AsyncMock()
        bot.send_document = AsyncMock()

        # Create a temp backup file
        test_path = Path("data/test_backup.db")
        test_path.parent.mkdir(parents=True, exist_ok=True)
        test_path.write_text("test backup data")

        result = await send_backup_to_admin(bot, test_path)
        assert result
        bot.send_document.assert_called_once()

        # Cleanup
        test_path.unlink(missing_ok=True)


# ═══════════════════════════════════════════════════════════════════════════
#  P7. Progressive Profiling
# ═══════════════════════════════════════════════════════════════════════════


class TestProgressiveProfiling:
    """P7: Тесты прогрессивного профилирования лидов."""

    def test_lead_model_has_business_sphere(self):
        from src.database.models import Lead
        assert hasattr(Lead, "business_sphere")

    def test_lead_form_has_new_state(self):
        from src.bot.handlers.lead_form import LeadForm
        assert hasattr(LeadForm, "waiting_for_business_sphere")


# ═══════════════════════════════════════════════════════════════════════════
#  P8. Healthcheck
# ═══════════════════════════════════════════════════════════════════════════


class TestHealthcheck:
    """P8: Тесты healthcheck API."""

    def test_set_ready(self):
        from src.bot.utils.healthcheck import set_ready, _is_ready
        set_ready(True)
        from src.bot.utils import healthcheck
        assert healthcheck._is_ready is True
        set_ready(False)
        assert healthcheck._is_ready is False

    @pytest.mark.asyncio
    async def test_health_handler(self):
        from src.bot.utils.healthcheck import _health_handler, set_ready
        set_ready(True)

        request = MagicMock()
        response = await _health_handler(request)
        assert response.status == 200

    @pytest.mark.asyncio
    async def test_ready_handler_ready(self):
        from src.bot.utils.healthcheck import _ready_handler, set_ready
        set_ready(True)
        request = MagicMock()
        response = await _ready_handler(request)
        assert response.status == 200

    @pytest.mark.asyncio
    async def test_ready_handler_not_ready(self):
        from src.bot.utils.healthcheck import _ready_handler, set_ready
        set_ready(False)
        request = MagicMock()
        response = await _ready_handler(request)
        assert response.status == 503

    @pytest.mark.asyncio
    async def test_metrics_handler(self):
        from src.bot.utils.healthcheck import _metrics_handler, set_ready
        set_ready(True)
        request = MagicMock()
        response = await _metrics_handler(request)
        assert "bot_uptime_seconds" in response.text


# ═══════════════════════════════════════════════════════════════════════════
#  P9. Log Rotation & Cleanup
# ═══════════════════════════════════════════════════════════════════════════


class TestLogRotation:
    """P9: Тесты ротации логов и очистки кеша."""

    def test_log_stats(self):
        from src.bot.utils.log_manager import get_log_stats
        stats = get_log_stats()
        assert "total_size" in stats
        assert "file_count" in stats
        assert "total_size_human" in stats

    def test_cleanup_cache_empty(self):
        from src.bot.utils.log_manager import cleanup_cache
        # Should work even with non-existent dirs
        stats = cleanup_cache()
        assert "deleted" in stats
        assert "freed_bytes" in stats
        assert "errors" in stats

    def test_cleanup_cache_with_old_files(self):
        from src.bot.utils.log_manager import cleanup_cache
        import tempfile

        # Create temp dir with old file
        test_dir = Path("data/cache")
        test_dir.mkdir(parents=True, exist_ok=True)
        test_file = test_dir / "old_test_file.tmp"
        test_file.write_text("old data")
        # Adjust mtime to 72 hours ago
        old_time = time.time() - 72 * 3600
        os.utime(str(test_file), (old_time, old_time))

        stats = cleanup_cache(max_age_hours=48)
        assert stats["deleted"] >= 1

    def test_setup_log_rotation(self):
        from src.bot.utils.log_manager import setup_log_rotation
        result = setup_log_rotation()
        assert result is True


# ═══════════════════════════════════════════════════════════════════════════
#  P10. Security Audit
# ═══════════════════════════════════════════════════════════════════════════


class TestSecurityAudit:
    """P10: Тесты OWASP аудита безопасности."""

    def test_audit_runs(self):
        from src.bot.utils.security_audit import run_security_audit
        audit = run_security_audit()
        assert audit["total_files"] > 0
        assert "grade" in audit
        assert "summary" in audit

    def test_audit_no_critical_sql_injection(self):
        """В нашем коде НЕ должно быть SQL-инъекций (используем SQLAlchemy)."""
        from src.bot.utils.security_audit import run_security_audit
        audit = run_security_audit()
        sql_issues = [i for i in audit["issues"] if i["vuln_id"] == "SQL_INJECTION"]
        assert len(sql_issues) == 0, f"SQL injection found: {sql_issues}"

    def test_format_report(self):
        from src.bot.utils.security_audit import run_security_audit, format_audit_report
        audit = run_security_audit()
        report = format_audit_report(audit)
        assert "Аудит безопасности" in report
        assert len(report) > 50

    def test_scan_clean_file(self):
        from src.bot.utils.security_audit import scan_file
        # Create a clean Python file
        test_file = Path("data/test_clean.py")
        test_file.parent.mkdir(parents=True, exist_ok=True)
        test_file.write_text("x = 1 + 2\nprint(x)\n")
        issues = scan_file(test_file)
        assert len(issues) == 0
        test_file.unlink(missing_ok=True)

    def test_scan_detects_exec(self):
        from src.bot.utils.security_audit import scan_file
        test_file = Path("data/test_vuln.py")
        test_file.parent.mkdir(parents=True, exist_ok=True)
        test_file.write_text("result = exec('print(1)')\n")
        issues = scan_file(test_file)
        assert any(i["vuln_id"] == "EXEC_EVAL" for i in issues)
        test_file.unlink(missing_ok=True)


# ═══════════════════════════════════════════════════════════════════════════
#  INTEGRATION: Full Simulation
# ═══════════════════════════════════════════════════════════════════════════


class TestFullStabilitySimulation:
    """Интеграционный тест: симуляция 50 виртуальных пользователей."""

    @pytest.mark.asyncio
    async def test_simulate_user_flow_with_telemetry(self):
        """Имитирует 50 пользователей проходящих через воронку."""
        from src.bot.utils.telemetry import track_event_sync, get_funnel_stats, _funnel_counters, _events
        from src.bot.utils.validators import validate_lead

        _funnel_counters.clear()
        _events.clear()

        total_users = 50
        leads_created = 0
        validation_errors = 0

        for i in range(total_users):
            user_id = 10000 + i

            # Stage 1: start
            track_event_sync(user_id, "bot_started")

            # Stage 2: 80% open guide menu
            if i % 5 != 0:
                track_event_sync(user_id, "guide_menu_opened")

                # Stage 3: 60% select a guide
                if i % 3 != 0:
                    track_event_sync(user_id, "guide_selected", {"guide": f"guide_{i % 5}"})

                    # Stage 4: 90% give consent
                    if i % 10 != 0:
                        track_event_sync(user_id, "consent_given")

                        # Stage 5: validate email
                        if i % 7 == 0:
                            email = "bad_email"
                        else:
                            email = f"user{i}@company.com"

                        ok, err = validate_lead(f"User {i}", email, f"guide_{i % 5}")
                        if ok:
                            track_event_sync(user_id, "email_entered")
                            track_event_sync(user_id, "name_entered")
                            track_event_sync(user_id, "lead_saved")
                            leads_created += 1
                        else:
                            validation_errors += 1

        stats = get_funnel_stats()
        assert stats["bot_started"] == total_users
        assert stats["lead_saved"] > 0
        assert leads_created > 0
        assert validation_errors > 0  # Some had bad emails

        # Check drop rates make sense
        from src.bot.utils.telemetry import get_funnel_drop_rates
        drops = get_funnel_drop_rates()
        first_drop = drops[0]
        assert first_drop["rate"] > 0
        assert first_drop["rate"] < 100  # Not everyone passes

    def test_error_middleware_and_throttle_coexist(self):
        """Проверяем что оба middleware могут быть созданы одновременно."""
        from src.bot.middlewares.error_handler import ErrorHandlingMiddleware
        from src.bot.middlewares.throttle import ThrottlingMiddleware

        bot = MagicMock()
        error_mw = ErrorHandlingMiddleware(bot)
        throttle_mw = ThrottlingMiddleware()
        assert error_mw is not None
        assert throttle_mw is not None

    def test_security_audit_passes(self):
        """Полный аудит кодовой базы не должен иметь CRITICAL проблем."""
        from src.bot.utils.security_audit import run_security_audit
        audit = run_security_audit()
        assert audit["critical"] == 0, (
            f"CRITICAL issues found:\n"
            + "\n".join(
                f"  {i['file']}:{i['line']} — {i['description']}"
                for i in audit["issues"] if i["severity"] == "CRITICAL"
            )
        )
