"""Скрипт самопроверки системы — запуск после каждого изменения.

Проверяет:
1. Импорт всех модулей
2. Конфигурация
3. Pydantic-валидаторы
4. Телеметрия
5. Security audit
6. Healthcheck (mock)
7. Error handler
8. Throttle middleware
9. Log rotation
10. DB models

Использование:
    python verify_system.py
"""

import asyncio
import sys
import traceback

CHECKS = []
PASSED = 0
FAILED = 0


def check(name: str):
    """Декоратор для регистрации проверки."""
    def decorator(func):
        CHECKS.append((name, func))
        return func
    return decorator


@check("P1: ErrorHandlingMiddleware imports")
def test_error_middleware():
    from src.bot.middlewares.error_handler import ErrorHandlingMiddleware, get_error_stats, _mask_secrets
    assert callable(ErrorHandlingMiddleware)
    assert callable(get_error_stats)
    assert "***" in _mask_secrets("sk-proj-12345678901234567890")


@check("P2: ThrottlingMiddleware imports")
def test_throttle():
    from src.bot.middlewares.throttle import ThrottlingMiddleware, get_throttle_stats
    assert callable(ThrottlingMiddleware)
    stats = get_throttle_stats()
    assert "active_bans" in stats


@check("P3: Pydantic validators")
def test_validators():
    from src.bot.utils.validators import (
        validate_lead, validate_article, is_garbage_text,
        is_valid_url, check_config_sanity, LeadValidator,
    )
    # Valid lead
    ok, err = validate_lead("Алексей", "test@example.com", "esop")
    assert ok, f"Valid lead rejected: {err}"

    # Invalid email
    ok, err = validate_lead("Алексей", "notanemail", "esop")
    assert not ok, "Invalid email accepted"

    # Garbage name
    assert is_garbage_text("asdfgh")
    assert is_garbage_text("a")
    assert not is_garbage_text("Алексей Петров")

    # Valid article
    ok, err = validate_article("Test Title Here", "A" * 60)
    assert ok

    # URL validation
    assert is_valid_url("https://example.com/path")
    assert not is_valid_url("not a url")


@check("P4: Sentry integration (noop)")
def test_sentry():
    from src.bot.utils.sentry_integration import (
        init_sentry, capture_exception, capture_message, is_enabled,
    )
    # Without DSN, should return False
    result = init_sentry()
    assert not result or isinstance(result, bool)
    assert not is_enabled() or isinstance(is_enabled(), bool)

    # Should not crash even without sentry
    try:
        capture_exception(ValueError("test"))
        capture_message("test message")
    except Exception as e:
        raise AssertionError(f"Sentry noop failed: {e}")


@check("P5: Telemetry")
def test_telemetry():
    from src.bot.utils.telemetry import (
        track_event_sync, get_funnel_stats, get_funnel_drop_rates,
        get_recent_events, FUNNEL_STAGES,
    )
    # Track some events
    track_event_sync(123, "bot_started")
    track_event_sync(123, "guide_selected", {"guide": "esop"})
    track_event_sync(123, "consent_given")
    track_event_sync(123, "lead_saved")

    stats = get_funnel_stats()
    assert stats["bot_started"] >= 1
    assert stats["guide_selected"] >= 1

    drops = get_funnel_drop_rates()
    assert len(drops) > 0

    events = get_recent_events(10)
    assert len(events) >= 4


@check("P6: Backup module")
def test_backup():
    from src.backup import (
        vacuum_database, create_backup, cleanup_old_backups,
        scheduled_backup, daily_backup, send_backup_to_admin,
    )
    assert callable(vacuum_database)
    assert callable(daily_backup)
    assert callable(send_backup_to_admin)


@check("P7: Lead model has business_sphere")
def test_progressive_profiling():
    from src.database.models import Lead
    # Check that the field exists
    assert hasattr(Lead, "business_sphere")


@check("P8: Healthcheck")
def test_healthcheck():
    from src.bot.utils.healthcheck import (
        start_healthcheck, stop_healthcheck, set_ready,
        _health_handler, _ready_handler,
    )
    assert callable(start_healthcheck)
    assert callable(stop_healthcheck)
    set_ready(True)
    set_ready(False)


@check("P9: Log rotation")
def test_log_rotation():
    from src.bot.utils.log_manager import (
        setup_log_rotation, cleanup_cache, get_log_stats, scheduled_cleanup,
    )
    assert callable(setup_log_rotation)
    stats = get_log_stats()
    assert "total_size" in stats


@check("P10: Security audit")
def test_security_audit():
    from src.bot.utils.security_audit import (
        run_security_audit, format_audit_report, scan_file,
    )
    audit = run_security_audit()
    assert "total_files" in audit
    assert "total_issues" in audit
    assert "grade" in audit
    assert audit["total_files"] > 0

    report = format_audit_report(audit)
    assert "Аудит" in report


@check("Config has new fields")
def test_config():
    from src.config import settings
    assert hasattr(settings, "SENTRY_DSN")
    assert hasattr(settings, "ENVIRONMENT")
    assert hasattr(settings, "VERSION")


@check("All handlers import")
def test_handlers_import():
    from src.bot.handlers import (
        admin, broadcast, cabinet, consult, content_manager,
        documents, feedback, group_mode, language, lead_form,
        live_support, payments, referral, start, subscription,
        timezone_handler, voice, waitlist_handler,
    )
    from src.bot.handlers import digest, strategy


def main():
    global PASSED, FAILED

    print("=" * 60)
    print("  SOLIS Partners Bot — System Verification")
    print("=" * 60)
    print()

    for name, func in CHECKS:
        try:
            func()
            PASSED += 1
            print(f"  ✅ {name}")
        except Exception as e:
            FAILED += 1
            print(f"  ❌ {name}: {e}")
            traceback.print_exc()

    print()
    print(f"  Results: {PASSED} passed, {FAILED} failed / {len(CHECKS)} total")
    print("=" * 60)

    if FAILED > 0:
        sys.exit(1)
    print("  🎉 All checks passed!")


if __name__ == "__main__":
    main()
