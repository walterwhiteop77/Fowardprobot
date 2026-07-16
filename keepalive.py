"""
Keep-Alive Module
─────────────────
Starts a lightweight aiohttp web server so Render (and similar free hosts)
can see a live HTTP port.  A background ping loop calls PING_URL every
4 minutes so the service is never considered idle.

Setup on Render:
  1. Set PING_URL to your Render service URL, e.g.
       https://my-forward-bot.onrender.com
  2. That's it — no external ping service required.
     (You can also add UptimeRobot pointing at the same URL for extra safety.)
"""

import asyncio
import logging
from aiohttp import web, ClientSession, ClientTimeout
from config import Config

logger = logging.getLogger(__name__)

PING_INTERVAL = 240   # seconds between pings (4 min < Render's 15-min idle limit)
PING_TIMEOUT  = 10    # seconds before a ping attempt is considered failed


# ── HTTP health handlers ──────────────────────────────────────────────────────

async def _root(request):
    return web.Response(
        text=(
            "✅ Bot is alive!\n\n"
            f"Service: ForwardProBot\n"
            f"Auto-forward: enabled\n"
        ),
        content_type="text/plain",
    )

async def _health(request):
    return web.json_response({"status": "ok", "service": "ForwardProBot"})


# ── Self-ping loop ────────────────────────────────────────────────────────────

async def _ping_loop():
    url = Config.PING_URL
    if not url:
        logger.info("[keepalive] PING_URL not set — self-ping disabled. "
                    "Set PING_URL=https://<your-render-service>.onrender.com "
                    "to keep the bot alive on free tier.")
        return

    logger.info(f"[keepalive] Self-ping enabled → {url}  (every {PING_INTERVAL}s)")
    await asyncio.sleep(30)   # give the server time to fully start

    timeout = ClientTimeout(total=PING_TIMEOUT)
    async with ClientSession(timeout=timeout) as session:
        while True:
            try:
                async with session.get(url) as resp:
                    logger.info(f"[keepalive] ping {url} → HTTP {resp.status}")
            except Exception as e:
                logger.warning(f"[keepalive] ping failed: {e}")
            await asyncio.sleep(PING_INTERVAL)


# ── Public entry point ────────────────────────────────────────────────────────

async def start_keepalive():
    """
    Call once inside your asyncio main, after the bot has started.
    Starts the HTTP server on Config.PORT and launches the ping loop.
    """
    app = web.Application()
    app.router.add_get("/",       _root)
    app.router.add_get("/health", _health)

    runner = web.AppRunner(app)
    await runner.setup()

    port = Config.PORT
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    logger.info(f"[keepalive] HTTP server listening on port {port}")

    asyncio.create_task(_ping_loop())
