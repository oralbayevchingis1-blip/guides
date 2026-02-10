"""P8. Healthcheck HTTP API для Docker.

Запускает легковесный HTTP-сервер (aiohttp.web), который отдаёт:
- GET /health → 200 OK + JSON с метриками бота
- GET /ready → 200 только если бот подключён к Telegram

Используется Docker healthcheck:
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8081/health"]
      interval: 30s
      timeout: 5s
      retries: 3

Использование:
    from src.bot.utils.healthcheck import start_healthcheck, stop_healthcheck
    await start_healthcheck(bot)     # при старте
    await stop_healthcheck()         # при остановке
"""

import logging
import time
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

_start_time = time.time()
_runner = None
_site = None
_bot_ref = None
_is_ready = False

HEALTHCHECK_PORT = 8081


async def _health_handler(request):
    """Основной healthcheck: жив ли процесс."""
    from aiohttp import web

    uptime = int(time.time() - _start_time)
    now = datetime.now(timezone.utc).isoformat()

    data = {
        "status": "ok",
        "uptime_seconds": uptime,
        "timestamp": now,
        "bot_connected": _is_ready,
        "version": "2.0.0",
    }

    # Доп. метрики если доступны
    try:
        from src.bot.middlewares.error_handler import get_error_stats
        data["error_counts"] = get_error_stats()
    except Exception:
        pass

    try:
        from src.bot.middlewares.throttle import get_throttle_stats
        data["throttle"] = get_throttle_stats()
    except Exception:
        pass

    return web.json_response(data)


async def _ready_handler(request):
    """Readiness probe: бот подключён к Telegram."""
    from aiohttp import web

    if _is_ready:
        return web.json_response({"status": "ready"})
    return web.json_response({"status": "not_ready"}, status=503)


async def _metrics_handler(request):
    """Простые метрики в текстовом формате (Prometheus-like)."""
    from aiohttp import web

    uptime = int(time.time() - _start_time)
    lines = [
        f"# HELP bot_uptime_seconds Total uptime",
        f"# TYPE bot_uptime_seconds gauge",
        f"bot_uptime_seconds {uptime}",
        f"# HELP bot_ready Is bot connected to Telegram",
        f"# TYPE bot_ready gauge",
        f"bot_ready {1 if _is_ready else 0}",
    ]

    try:
        from src.bot.middlewares.error_handler import get_error_stats
        for exc_type, count in get_error_stats().items():
            safe = exc_type.replace(" ", "_")
            lines.append(f'bot_errors_total{{type="{safe}"}} {count}')
    except Exception:
        pass

    return web.Response(text="\n".join(lines), content_type="text/plain")


async def start_healthcheck(bot=None, port: int = HEALTHCHECK_PORT) -> bool:
    """Запускает HTTP-сервер healthcheck.

    Args:
        bot: Экземпляр aiogram Bot (для readiness check).
        port: Порт HTTP-сервера.

    Returns:
        True если сервер запущен.
    """
    global _runner, _site, _bot_ref, _is_ready

    try:
        from aiohttp import web

        _bot_ref = bot
        _is_ready = bot is not None

        app = web.Application()
        app.router.add_get("/health", _health_handler)
        app.router.add_get("/ready", _ready_handler)
        app.router.add_get("/metrics", _metrics_handler)

        _runner = web.AppRunner(app)
        await _runner.setup()
        _site = web.TCPSite(_runner, "0.0.0.0", port)
        await _site.start()

        logger.info("Healthcheck server started on port %d", port)
        return True

    except ImportError:
        logger.info("aiohttp not available — healthcheck disabled")
        return False
    except OSError as e:
        logger.warning("Could not start healthcheck on port %d: %s", port, e)
        return False
    except Exception as e:
        logger.error("Healthcheck start failed: %s", e)
        return False


async def stop_healthcheck() -> None:
    """Останавливает HTTP-сервер healthcheck."""
    global _runner, _site

    if _runner:
        try:
            await _runner.cleanup()
            logger.info("Healthcheck server stopped")
        except Exception as e:
            logger.error("Healthcheck stop error: %s", e)
        finally:
            _runner = None
            _site = None


def set_ready(ready: bool = True) -> None:
    """Устанавливает readiness-статус бота."""
    global _is_ready
    _is_ready = ready
