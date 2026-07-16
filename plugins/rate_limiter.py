"""
rate_limiter.py — Shared Telegram send-rate protection
=======================================================

Used by BOTH the auto-forward engine (af_engine.py)
and the manual bulk-forward plugin (regix.py).

Architecture
────────────
                    safe_copy_message()
                    safe_forward_messages()
                    safe_send_cached_media()
                           │
               ┌───────────┴───────────┐
         GlobalBucket             ChannelBucket
       (per Pyrogram client)    (per target chat_id)
               │                       │
        token-bucket              token-bucket
        refills @ global_rate     refills @ channel_rate
               │                       │
               └───────────┬───────────┘
                     Circuit-Breaker
                  (per target chat_id)
                           │
                   Telegram API call
                           │
              ┌────────────┴────────────┐
         Success                   Error handling
         cb.ok()          FloodWait / SlowModeWait
                          PeerFlood / Permanent
                          exponential back-off + jitter

Rate limits (conservative — well below Telegram's actual ceilings)
───────────────────────────────────────────────────────────────────
Per channel  : 1 message every 5 s  (≈12/min;  Telegram ceiling ≈20/min)
Global bot   : 15 sends/s           (Telegram ceiling ≈30/s)
Global UB    :  4 sends/s           (user accounts are watched more closely)
Jitter       : +0.3 – 1.5 s random on every send
"""

import asyncio
import logging
import random
import time
from dataclasses import dataclass, field
from typing import Dict, Optional

from pyrogram import Client
from pyrogram.errors import FloodWait, RPCError

logger = logging.getLogger(__name__)

# ── Try to import optional error types (present in pyrofork / pyrogram) ───────
try:
    from pyrogram.errors import SlowModeWait
except ImportError:
    SlowModeWait = None        # type: ignore

try:
    from pyrogram.errors import PeerFlood
except ImportError:
    PeerFlood = None           # type: ignore

try:
    from pyrogram.errors import ChatWriteForbidden
except ImportError:
    ChatWriteForbidden = None  # type: ignore

try:
    from pyrogram.errors import ChannelPrivate
except ImportError:
    ChannelPrivate = None      # type: ignore

# ── Tuneable constants ────────────────────────────────────────────────────────

PER_CHANNEL_RATE  = 1 / 5    # token/s  ≈ 12 msgs/min per target channel
PER_CHANNEL_BURST = 2        # max burst before throttling kicks in

GLOBAL_BOT_RATE   = 15.0     # global msgs/s for bot clients
GLOBAL_UB_RATE    =  4.0     # global msgs/s for userbot clients

JITTER_MIN =  0.3            # seconds
JITTER_MAX =  1.5            # seconds

MAX_RETRIES = 6              # attempts before giving up on one message

CB_FAIL_THRESHOLD = 5        # consecutive failures before circuit-breaker trips
CB_PAUSE_SECONDS  = 600      # 10 minutes pause when circuit-breaker trips

PEER_FLOOD_COOLDOWN = 3600   # 1 hour cooldown after PeerFlood

# ── Token Bucket ──────────────────────────────────────────────────────────────

class TokenBucket:
    """
    Async token bucket.

    Tokens refill at `rate` per second up to `capacity`.
    acquire() waits until ≥1 token is available, then consumes one.
    drain(extra) empties the bucket and blocks future acquisitions
    for `extra` additional seconds (used after FloodWait).
    """

    def __init__(self, rate: float, capacity: float):
        self._rate     = rate
        self._capacity = capacity
        self._tokens   = capacity     # start full
        self._last     = time.monotonic()
        self._lock     = asyncio.Lock()

    async def acquire(self) -> None:
        async with self._lock:
            now     = time.monotonic()
            elapsed = max(0.0, now - self._last)   # never go negative
            self._tokens = min(self._capacity,
                               self._tokens + elapsed * self._rate)
            self._last = now

            if self._tokens >= 1.0:
                self._tokens -= 1.0
                wait = 0.0
            else:
                wait = (1.0 - self._tokens) / self._rate
                self._tokens = 0.0
                self._last  += wait       # advance clock

        if wait > 0:
            logger.debug(f"[rate] bucket wait {wait:.2f}s")
            await asyncio.sleep(wait)

    def drain(self, extra_seconds: float = 0.0) -> None:
        """Empty the bucket and optionally freeze it for `extra_seconds`."""
        self._tokens = 0.0
        if extra_seconds > 0:
            self._last = time.monotonic() + extra_seconds


# ── Per-channel and per-client bucket registries ──────────────────────────────

_channel_buckets: Dict[int, TokenBucket] = {}
_channel_lock = asyncio.Lock()

_global_buckets: Dict[int, TokenBucket] = {}
_global_lock = asyncio.Lock()


async def _ch_bucket(chat_id: int) -> TokenBucket:
    async with _channel_lock:
        if chat_id not in _channel_buckets:
            _channel_buckets[chat_id] = TokenBucket(
                PER_CHANNEL_RATE, PER_CHANNEL_BURST
            )
        return _channel_buckets[chat_id]


async def _gl_bucket(client: Client, is_userbot: bool = False) -> TokenBucket:
    key = id(client)
    async with _global_lock:
        if key not in _global_buckets:
            rate = GLOBAL_UB_RATE if is_userbot else GLOBAL_BOT_RATE
            _global_buckets[key] = TokenBucket(rate, capacity=rate)
        return _global_buckets[key]


# ── Circuit Breaker ───────────────────────────────────────────────────────────

@dataclass
class _CB:
    failures:     int   = 0
    paused_until: float = 0.0

    def is_tripped(self) -> bool:
        now = time.monotonic()
        if now < self.paused_until:
            return True
        if self.paused_until > 0:        # just recovered
            self.failures     = 0
            self.paused_until = 0.0
        return False

    def fail(self, label: str = ""):
        self.failures += 1
        if self.failures >= CB_FAIL_THRESHOLD:
            self.paused_until = time.monotonic() + CB_PAUSE_SECONDS
            logger.warning(
                f"[rate] circuit-breaker TRIPPED {label} "
                f"— pausing {CB_PAUSE_SECONDS}s"
            )

    def ok(self):
        self.failures     = 0
        self.paused_until = 0.0


_cbs: Dict[int, _CB] = {}

def _cb(chat_id: int) -> _CB:
    if chat_id not in _cbs:
        _cbs[chat_id] = _CB()
    return _cbs[chat_id]


# ── Jitter helper ─────────────────────────────────────────────────────────────

async def _jitter():
    await asyncio.sleep(random.uniform(JITTER_MIN, JITTER_MAX))


def _is_permanent(exc: Exception) -> bool:
    """True if the error means we must never retry for this target."""
    permanent = []
    if ChatWriteForbidden:
        permanent.append(ChatWriteForbidden)
    if ChannelPrivate:
        permanent.append(ChannelPrivate)
    # Also catch by name for safety
    name = type(exc).__name__
    permanent_names = {
        "ChatWriteForbidden", "ChannelPrivate", "UserBannedInChannel",
        "BroadcastForbidden", "ChatSendMediaForbidden",
        "ChatSendDocForbidden", "ChatSendPhotoForbidden",
        "BannedRightsException",
    }
    return isinstance(exc, tuple(permanent)) or name in permanent_names


def _is_missing(exc: Exception) -> bool:
    """True if the source message no longer exists (deleted)."""
    name = type(exc).__name__
    return name in {"MessageIdInvalid", "MsgIdInvalid", "MessageEmpty"}


# ── Core safe-send wrappers ───────────────────────────────────────────────────

async def safe_copy_message(
    client: Client,
    *,
    chat_id:          int,
    from_chat_id:     int,
    message_id:       int,
    caption:          Optional[str]  = None,
    caption_entities                 = None,
    is_userbot:       bool           = False,
    **kwargs,
) -> bool:
    """
    client.copy_message() with full rate-limit protection.
    Returns True on success, False if skipped permanently.
    """
    cb = _cb(chat_id)
    if cb.is_tripped():
        logger.warning(f"[rate] circuit-breaker open for {chat_id} — skipping")
        return False

    gl = await _gl_bucket(client, is_userbot)
    ch = await _ch_bucket(chat_id)

    call_kwargs = {"chat_id": chat_id, "from_chat_id": from_chat_id,
                   "message_id": message_id, **kwargs}
    if caption is not None:
        call_kwargs["caption"] = caption
    if caption_entities is not None:
        call_kwargs["caption_entities"] = caption_entities

    for attempt in range(MAX_RETRIES):
        await gl.acquire()    # global rate limit first
        await ch.acquire()    # then per-channel
        await _jitter()       # random spread

        try:
            await client.copy_message(**call_kwargs)
            cb.ok()
            logger.info(f"[rate] ✓ copy {message_id} {from_chat_id}→{chat_id}")
            return True

        except FloodWait as e:
            wait = e.value + random.uniform(3, 6)
            logger.warning(
                f"[FloodWait] {e.value}s → chat {chat_id} "
                f"(attempt {attempt+1}/{MAX_RETRIES}) — sleeping {wait:.1f}s"
            )
            gl.drain(wait)
            ch.drain(wait)
            await asyncio.sleep(wait)

        except Exception as e:
            if SlowModeWait and isinstance(e, SlowModeWait):
                wait = e.value + random.uniform(1, 3)
                logger.warning(f"[SlowMode] {e.value}s for {chat_id}")
                ch.drain(wait)
                await asyncio.sleep(wait)
                continue

            if PeerFlood and isinstance(e, PeerFlood):
                logger.error(
                    "[PeerFlood] ⚠️  Client flagged as spammer! "
                    "Freezing all sends for this client for 1 hour."
                )
                gl.drain(PEER_FLOOD_COOLDOWN)
                cb.fail(str(chat_id))
                await asyncio.sleep(PEER_FLOOD_COOLDOWN)
                return False

            if _is_permanent(e):
                logger.error(f"[Permanent] {type(e).__name__} → {chat_id}")
                cb.fail(str(chat_id))
                return False

            if _is_missing(e):
                logger.warning(f"[Missing] msg {message_id} no longer exists — skipping")
                return False

            # Transient RPC / network error
            logger.error(
                f"[RPCError] {type(e).__name__}: {e} → {chat_id} "
                f"(attempt {attempt+1}/{MAX_RETRIES})"
            )
            cb.fail(str(chat_id))
            if attempt < MAX_RETRIES - 1:
                backoff = min(120, (2 ** attempt) + random.uniform(0, 3))
                await asyncio.sleep(backoff)

    logger.error(
        f"[Exhausted] gave up on msg {message_id} → {chat_id} "
        f"after {MAX_RETRIES} attempts"
    )
    cb.fail(str(chat_id))
    return False


async def safe_send_cached_media(
    client: Client,
    *,
    chat_id:      int,
    file_id:      str,
    caption:      Optional[str] = None,
    reply_markup                = None,
    protect_content: bool       = False,
    is_userbot:   bool          = False,
    **kwargs,
) -> bool:
    """
    client.send_cached_media() with full rate-limit protection.
    """
    cb = _cb(chat_id)
    if cb.is_tripped():
        return False

    gl = await _gl_bucket(client, is_userbot)
    ch = await _ch_bucket(chat_id)

    call_kwargs = {
        "chat_id": chat_id, "file_id": file_id,
        "protect_content": protect_content, **kwargs,
    }
    if caption is not None:
        call_kwargs["caption"] = caption
    if reply_markup is not None:
        call_kwargs["reply_markup"] = reply_markup

    for attempt in range(MAX_RETRIES):
        await gl.acquire()
        await ch.acquire()
        await _jitter()

        try:
            await client.send_cached_media(**call_kwargs)
            cb.ok()
            return True

        except FloodWait as e:
            wait = e.value + random.uniform(3, 6)
            logger.warning(f"[FloodWait] {e.value}s send_cached → {chat_id}")
            gl.drain(wait)
            ch.drain(wait)
            await asyncio.sleep(wait)

        except Exception as e:
            if SlowModeWait and isinstance(e, SlowModeWait):
                wait = e.value + random.uniform(1, 3)
                ch.drain(wait)
                await asyncio.sleep(wait)
                continue
            if PeerFlood and isinstance(e, PeerFlood):
                gl.drain(PEER_FLOOD_COOLDOWN)
                await asyncio.sleep(PEER_FLOOD_COOLDOWN)
                return False
            if _is_permanent(e):
                cb.fail(str(chat_id))
                return False
            logger.error(f"[Error] send_cached {type(e).__name__}: {e} → {chat_id}")
            if attempt < MAX_RETRIES - 1:
                await asyncio.sleep(min(120, 2 ** attempt + random.uniform(0, 3)))

    return False


async def safe_forward_messages(
    client: Client,
    *,
    chat_id:        int,
    from_chat_id:   int,
    message_ids,
    protect_content: bool = False,
    is_userbot:     bool  = False,
    **kwargs,
) -> bool:
    """
    client.forward_messages() with full rate-limit protection.
    """
    cb = _cb(chat_id)
    if cb.is_tripped():
        return False

    gl = await _gl_bucket(client, is_userbot)
    ch = await _ch_bucket(chat_id)

    for attempt in range(MAX_RETRIES):
        await gl.acquire()
        await ch.acquire()
        await _jitter()

        try:
            await client.forward_messages(
                chat_id=chat_id,
                from_chat_id=from_chat_id,
                message_ids=message_ids,
                protect_content=protect_content,
                **kwargs,
            )
            cb.ok()
            return True

        except FloodWait as e:
            wait = e.value + random.uniform(3, 6)
            logger.warning(f"[FloodWait] {e.value}s forward → {chat_id}")
            gl.drain(wait)
            ch.drain(wait)
            await asyncio.sleep(wait)

        except Exception as e:
            if SlowModeWait and isinstance(e, SlowModeWait):
                wait = e.value + random.uniform(1, 3)
                ch.drain(wait)
                await asyncio.sleep(wait)
                continue
            if PeerFlood and isinstance(e, PeerFlood):
                gl.drain(PEER_FLOOD_COOLDOWN)
                await asyncio.sleep(PEER_FLOOD_COOLDOWN)
                return False
            if _is_permanent(e):
                cb.fail(str(chat_id))
                return False
            logger.error(f"[Error] forward {type(e).__name__}: {e} → {chat_id}")
            if attempt < MAX_RETRIES - 1:
                await asyncio.sleep(min(120, 2 ** attempt + random.uniform(0, 3)))

    cb.fail(str(chat_id))
    return False
