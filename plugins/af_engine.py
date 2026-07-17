"""
Auto-Forward Engine
───────────────────
Two forwarding paths:

  BOT path     — handles source channels where the main bot is admin.
                 Skipped for any user whose userbot is already running
                 (avoids duplicate forwards).

  USERBOT path — one Pyrogram Client per user with a saved userbot
                 session.  Handles ALL their source channels, including
                 private ones the main bot cannot join.

On every incoming channel message each path:
  1. Checks that the chat is a configured source for this user.
  2. Runs the user's AF filter (types / size / keywords / extensions).
  3. Calls copy_message() to each target through a per-target lock so that
     bulk/album bursts are serialised — no concurrent FloodWait races.

How burst / album forwarding is handled
────────────────────────────────────────
Each target channel has its own asyncio.Lock.  When 5-6 files arrive
simultaneously they all queue up on that lock, so only one copy_message
is ever in-flight to a given target at any instant.

After every successful send the lock is held for an additional pause
(controlled by the user's Speed setting) before the next message is
allowed through.  This prevents rapid-fire sends from triggering
Telegram's per-chat rate limits.

A minimum pause of 0.5 s is enforced even in "fast" mode.

FloodWait retries up to 5 times (not just once) so that a second or
third throttle during a burst does not silently drop the message.

Speed mode inter-message delay (inside the per-target lock)
────────────────────────────────────────────────────────────
  safe   → 3.0 s  (max protection, lowest ban risk)
  normal → 1.0 s  (balanced, recommended)
  fast   → 0.5 s  (minimum floor — still avoids burst rate-limits)
"""

import asyncio
import logging
import re
from typing import Dict

from pyrogram import Client, filters
from pyrogram.errors import FloodWait
from pyrogram.handlers import MessageHandler

from config import Config
from database import db, AF_DEFAULT_FILTERS, AF_DEFAULT_SPEED

logger = logging.getLogger(__name__)

# ── Media filter ──────────────────────────────────────────────────────────────
_MEDIA = (
    filters.video
    | filters.document
    | filters.photo
    | filters.audio
    | filters.voice
    | filters.animation
)

# ── Inter-message delay inside the per-target lock ────────────────────────────
# Applied AFTER each successful send, BEFORE releasing the lock.
# This is the primary throttle for burst / album messages to the same target.
# A minimum floor of 0.5 s is enforced regardless of speed mode.
_MIN_DELAY = 0.5  # seconds — absolute minimum between sends to same target

SPEED_DELAY: dict = {
    "safe":   3.0,
    "normal": 1.0,
    "fast":   _MIN_DELAY,  # fast still respects the minimum floor
}

# ── Running userbot clients  {user_id → Client} ───────────────────────────────
_running_userbots: Dict[int, Client] = {}

# ── Per-target send locks ─────────────────────────────────────────────────────
# Serialises copy_message calls to the same target so that a burst of 5-6
# simultaneous messages doesn't cause concurrent FloodWait storms.
_target_locks: Dict[int, asyncio.Lock] = {}


def _get_target_lock(tid: int) -> asyncio.Lock:
    if tid not in _target_locks:
        _target_locks[tid] = asyncio.Lock()
    return _target_locks[tid]


# ── Filter helper ─────────────────────────────────────────────────────────────

def _passes_af_filter(message, af_cfg: dict) -> bool:
    """
    Return True if the message satisfies all active filter conditions.

    Checks:
      1. Media type is in the enabled-types list
      2. File size within [min_size_mb, max_size_mb]  (0 = no limit)
      3. Filename contains at least one keyword        (if set)
      4. File extension is in the allowed list         (if set)
    """
    if not message.media:
        return False

    media_type  = message.media.value          # e.g. "video", "document" …
    filters_cfg = af_cfg.get("filters", AF_DEFAULT_FILTERS)

    # 1. Type
    enabled_types = filters_cfg.get("types", AF_DEFAULT_FILTERS["types"])
    if media_type not in enabled_types:
        return False

    # Grab the media sub-object for size/name
    media_obj = getattr(message, media_type, None)
    file_size = getattr(media_obj, "file_size", 0) or 0
    file_name = getattr(media_obj, "file_name", "") or ""
    size_mb   = file_size / 1024 / 1024

    # 2. Size
    min_mb = float(filters_cfg.get("min_size_mb", 0) or 0)
    max_mb = float(filters_cfg.get("max_size_mb", 0) or 0)
    if min_mb > 0 and size_mb < min_mb:
        return False
    if max_mb > 0 and size_mb > max_mb:
        return False

    # 3. Keywords (filename must match at least one)
    keywords = filters_cfg.get("keywords", [])
    if keywords and file_name:
        pattern = "|".join(re.escape(k) for k in keywords)
        if not re.search(pattern, file_name, re.IGNORECASE):
            return False

    # 4. Extensions
    extensions = filters_cfg.get("extensions", [])
    if extensions and file_name and "." in file_name:
        ext     = file_name.rsplit(".", 1)[-1].lower()
        allowed = [e.lower().lstrip(".") for e in extensions]
        if ext not in allowed:
            return False

    return True


# ── Core send helper ──────────────────────────────────────────────────────────

async def _do_copy(
    client:      Client,
    tid:         int,
    msg,
    inter_delay: float = _MIN_DELAY,
    label:       str   = "",
) -> bool:
    """
    copy_message to a single target with full burst protection.

    1. Acquires a per-target lock — only one send in-flight to this target
       at any time.  All burst/album messages queue here.
    2. After a successful send, sleeps `inter_delay` seconds INSIDE the lock
       before releasing it.  This paces back-to-back messages so the next
       queued message cannot send immediately after the previous one, which
       would race into Telegram's per-chat rate limit.
    3. Retries FloodWait up to 5 times.  A single retry is not enough when
       several messages are queued — the 3rd and 4th message can hit a fresh
       FloodWait right after the previous retry finished.

    Returns True on success, False after exhausting retries.
    """
    lock = _get_target_lock(tid)
    async with lock:
        for attempt in range(5):
            try:
                await client.copy_message(
                    chat_id          = tid,
                    from_chat_id     = msg.chat.id,
                    message_id       = msg.id,
                    caption          = msg.caption,
                    caption_entities = msg.caption_entities if msg.caption else None,
                )
                # Throttle pause — held inside the lock so the next message
                # cannot start until this pause completes.
                await asyncio.sleep(inter_delay)
                return True

            except FloodWait as e:
                wait = e.value + 1
                logger.warning(
                    f"[af_engine]{label} FloodWait {e.value}s → {tid} "
                    f"(attempt {attempt + 1}/5), sleeping {wait}s…"
                )
                await asyncio.sleep(wait)

            except Exception as e:
                logger.error(
                    f"[af_engine]{label} copy_message failed → {tid}: {e}"
                )
                return False

        logger.error(
            f"[af_engine]{label} gave up on → {tid} after 5 FloodWait retries"
        )
        return False


async def _forward_to_targets(
    client:     Client,
    msg,
    target_ids: list,
    speed:      str,
    label:      str = "",
):
    """
    Forward one message to every target in target_ids.

    The inter-message delay (speed-mode pacing) is applied inside _do_copy,
    within the per-target lock, so it serves double duty:
      • paces sequential messages to the SAME target (burst / album)
      • paces a single message across MULTIPLE targets
    """
    delay = SPEED_DELAY.get(speed, SPEED_DELAY[AF_DEFAULT_SPEED])
    for tid in target_ids:
        await _do_copy(client, tid, msg, inter_delay=delay, label=label)


# ── Startup helpers ───────────────────────────────────────────────────────────

async def start_af_queue(bot_client: Client):
    """No-op kept for import compatibility with main.py."""
    logger.info("[af_engine] direct-forward mode active (no queue/buffer)")


async def start_all_userbot_af():
    """
    At bot startup: for every user with AF sources+targets and a saved
    userbot session, start one Pyrogram client to monitor their channels.
    """
    configs = await db.get_all_af_configs()
    started = 0
    for cfg in configs:
        uid = cfg.get("user_id")
        if not cfg.get("sources") or not cfg.get("targets"):
            continue
        ub_data = await db.get_userbot(uid)
        if not ub_data or not ub_data.get("session"):
            continue
        try:
            await start_userbot_af(uid, ub_data["session"])
            started += 1
        except Exception as e:
            logger.error(f"[af_engine] Could not start userbot for {uid}: {e}")
    logger.info(f"[af_engine] {started} userbot AF client(s) started at boot")


async def start_userbot_af(user_id: int, session_string: str) -> None:
    """Start (or restart) the userbot Pyrogram client for user_id."""
    await stop_userbot_af(user_id)

    ub = Client(
        f"af_ub_{user_id}",
        api_id         = Config.API_ID,
        api_hash       = Config.API_HASH,
        session_string = session_string,
        in_memory      = True,
    )

    async def _handler(ub_client: Client, message):
        try:
            source_id  = message.chat.id
            cfg        = await db.get_af_config(user_id)

            if not any(s["id"] == source_id for s in cfg.get("sources", [])):
                return

            target_ids = [t["id"] for t in cfg.get("targets", [])]
            if not target_ids:
                return

            if not _passes_af_filter(message, cfg):
                logger.debug(
                    f"[af_engine] ub: msg {message.id} filtered out for {user_id}"
                )
                return

            speed = cfg.get("speed", AF_DEFAULT_SPEED)
            logger.info(
                f"[af_engine] ub: forwarding msg {message.id} "
                f"from {source_id} → {target_ids} [speed={speed}]"
            )
            await _forward_to_targets(
                ub_client, message, target_ids, speed,
                label=f" [ub:{user_id}]",
            )
        except Exception as e:
            logger.error(
                f"[af_engine] userbot handler error (uid {user_id}): {e}",
                exc_info=True,
            )

    ub.add_handler(MessageHandler(_handler, filters.channel & _MEDIA))
    await ub.start()
    me = await ub.get_me()
    _running_userbots[user_id] = ub
    logger.info(
        f"[af_engine] userbot AF ready — user {user_id}, "
        f"account @{me.username or me.id}"
    )


async def stop_userbot_af(user_id: int) -> None:
    """Stop and remove the userbot client for user_id (if running)."""
    ub = _running_userbots.pop(user_id, None)
    if ub:
        try:
            await ub.stop()
        except Exception:
            pass
        logger.info(f"[af_engine] userbot AF stopped — user {user_id}")


# ── Bot-side channel handler ──────────────────────────────────────────────────
# Skipped for any user whose userbot is already handling the same source.

@Client.on_message(filters.channel & _MEDIA)
async def _bot_channel_handler(bot_client: Client, message):
    try:
        source_id    = message.chat.id
        user_entries = await db.get_source_users(source_id)
        if not user_entries:
            return

        for uid, tids, cfg in user_entries:
            # Skip — userbot is already watching for this user
            if uid in _running_userbots:
                continue

            if not _passes_af_filter(message, cfg):
                logger.debug(
                    f"[af_engine] bot: msg {message.id} filtered out for {uid}"
                )
                continue

            speed = cfg.get("speed", AF_DEFAULT_SPEED)
            logger.info(
                f"[af_engine] bot: queuing msg {message.id} "
                f"from {source_id} → {tids} [speed={speed}]"
            )
            # Each user's forward runs as a background task.
            # The per-target locks inside _do_copy ensure that burst messages
            # to the same target are serialised and properly throttled.
            asyncio.create_task(
                _forward_to_targets(
                    bot_client, message, tids, speed,
                    label=f" [bot→{uid}]",
                )
            )

    except Exception as e:
        logger.error(f"[af_engine] bot handler error: {e}", exc_info=True)
