"""
Auto-Forward Engine
───────────────────
Two forwarding paths share one async queue:

  BOT path     — handles source channels where the main bot is admin.
                 Active only for users who have NOT set up a userbot.

  USERBOT path — one Pyrogram Client per user who has a stored userbot
                 session.  Handles ALL source channels for that user,
                 including private ones the bot cannot read.

Filters  — per-user filter config (types, size, keywords, extensions)
           loaded from AF config in MongoDB and applied before buffering.

Speed    — per-user speed mode (safe / normal / fast) that controls
           extra inter-message delay added on top of the rate limiter.

           Mode     Extra delay/msg   Buffer flush
           safe     3.0 s             8 s
           normal   1.0 s             4 s
           fast     0.0 s             2 s

Rate limiting is fully delegated to plugins/rate_limiter.py:
  • Per-channel token bucket   (max 12 msgs/min per target channel)
  • Per-client global bucket   (max 15 msgs/s for bot, 4/s for userbot)
  • Circuit-breaker per target (pauses a broken target for 10 min)
  • FloodWait drain            (respects exact Telegram wait + jitter)
  • PeerFlood cooldown         (1-hour freeze when account is flagged)
"""

import asyncio
import logging
import re
from collections import defaultdict
from typing import Dict, List, Tuple

from pyrogram import Client, filters
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

# ── Speed constants ───────────────────────────────────────────────────────────
# Extra sleep added AFTER each safe_copy_message call, on top of the rate limiter.
SPEED_EXTRA_DELAY: dict = {
    "safe":   3.0,
    "normal": 1.0,
    "fast":   0.0,
}
# How long to buffer incoming messages before flushing to the queue.
# Longer = more complete media-group batching; shorter = lower latency.
SPEED_BUFFER_DELAY: dict = {
    "safe":   8.0,
    "normal": 4.0,
    "fast":   2.0,
}
DEFAULT_BUFFER_DELAY = SPEED_BUFFER_DELAY[AF_DEFAULT_SPEED]

# ── Running userbot clients  {user_id → Client} ───────────────────────────────
_running_userbots: Dict[int, Client] = {}

# ── Message buffer ────────────────────────────────────────────────────────────
# {source_chat_id → [(Message, [target_ids], Client, is_ub, speed)]}
_buffers:     Dict[int, List[Tuple]] = defaultdict(list)
_flush_tasks: Dict[int, asyncio.Task] = {}

# ── Forward queue  items: ([Message], [target_ids], Client, is_ub, speed) ─────
_queue: asyncio.Queue = asyncio.Queue()
_queue_running = False


# ── Filter helper ─────────────────────────────────────────────────────────────

def _passes_af_filter(message, af_cfg: dict) -> bool:
    """
    Return True if the message satisfies all active filter conditions.

    Checks (in order):
      1. Media type is in the enabled-types list
      2. File size is within [min_size_mb, max_size_mb] (0 = no limit)
      3. Filename contains at least one keyword (if keywords set)
      4. File extension is in the allowed list (if extensions set)
    """
    if not message.media:
        return False

    media_type  = message.media.value          # "video", "document", "photo", …
    filters_cfg = af_cfg.get("filters", AF_DEFAULT_FILTERS)

    # 1. Type filter
    enabled_types = filters_cfg.get("types", AF_DEFAULT_FILTERS["types"])
    if media_type not in enabled_types:
        return False

    # Fetch the media object for size/name checks
    media_obj = getattr(message, media_type, None)
    file_size = getattr(media_obj, "file_size", 0) or 0
    file_name = getattr(media_obj, "file_name", "") or ""
    size_mb   = file_size / 1024 / 1024

    # 2. Size filter
    min_mb = float(filters_cfg.get("min_size_mb", 0) or 0)
    max_mb = float(filters_cfg.get("max_size_mb", 0) or 0)
    if min_mb > 0 and size_mb < min_mb:
        return False
    if max_mb > 0 and size_mb > max_mb:
        return False

    # 3. Keyword filter (filename must match at least one keyword)
    keywords = filters_cfg.get("keywords", [])
    if keywords and file_name:
        pattern = "|".join(re.escape(k) for k in keywords)
        if not re.search(pattern, file_name, re.IGNORECASE):
            return False

    # 4. Extension filter
    extensions = filters_cfg.get("extensions", [])
    if extensions and file_name and "." in file_name:
        ext     = file_name.rsplit(".", 1)[-1].lower()
        allowed = [e.lower().lstrip(".") for e in extensions]
        if ext not in allowed:
            return False

    return True


# ── Queue worker ──────────────────────────────────────────────────────────────

async def _queue_worker():
    """
    Consume forwarding jobs one by one.
    Each job: (messages, target_ids, copy_client, is_userbot, speed).

    After every safe_copy_message call the speed-mode extra delay is applied,
    giving three distinct throughput levels on top of the hard rate limits.
    """
    while True:
        try:
            messages, target_ids, copy_client, is_ub, speed = await _queue.get()
            extra = SPEED_EXTRA_DELAY.get(speed, SPEED_EXTRA_DELAY[AF_DEFAULT_SPEED])

            for msg in sorted(messages, key=lambda m: m.id):
                for tid in target_ids:
                    try:
                        await copy_client.copy_message(
                            chat_id          = tid,
                            from_chat_id     = msg.chat.id,
                            message_id       = msg.id,
                            caption          = msg.caption,
                            caption_entities = msg.caption_entities if msg.caption else None,
                        )
                    except Exception as e:
                        from pyrogram.errors import FloodWait
                        if isinstance(e, FloodWait):
                            await asyncio.sleep(e.value)
                        else:
                            logger.error(f"[af_engine] copy_message error → {tid}: {e}")
                    if extra > 0:
                        await asyncio.sleep(extra)

            _queue.task_done()

        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"[af_engine] queue worker error: {e}", exc_info=True)
            await asyncio.sleep(1)


# ── Buffer helpers ────────────────────────────────────────────────────────────

async def _flush(source_id: int):
    """
    Wait for the buffer delay appropriate to the speed mode, then push the
    accumulated batch onto the queue.  The delay is taken from the FIRST
    item in the buffer so media groups with mixed users are handled correctly.
    """
    first = _buffers.get(source_id, [None])[0]
    if first:
        _, _, _, _, speed = first
        delay = SPEED_BUFFER_DELAY.get(speed, DEFAULT_BUFFER_DELAY)
    else:
        delay = DEFAULT_BUFFER_DELAY
    await asyncio.sleep(delay)

    items = _buffers.pop(source_id, [])
    _flush_tasks.pop(source_id, None)
    if not items:
        return

    # Group items by (frozenset of targets, client identity) to deduplicate
    groups: dict = {}
    for msg, target_ids, copy_client, is_ub, speed in items:
        key = (frozenset(target_ids), id(copy_client))
        if key not in groups:
            groups[key] = ([], list(target_ids), copy_client, is_ub, speed)
        groups[key][0].append(msg)

    for msgs, tids, cli, is_ub, speed in groups.values():
        await _queue.put((msgs, tids, cli, is_ub, speed))
        logger.info(
            f"[af_engine] queued {len(msgs)} msg(s) "
            f"from {source_id} → {len(tids)} target(s)  [speed={speed}]"
        )


def _add_to_buffer(
    source_id:   int,
    message,
    target_ids:  List[int],
    copy_client: Client,
    is_userbot:  bool,
    speed:       str,
):
    _buffers[source_id].append((message, target_ids, copy_client, is_userbot, speed))
    # Reset flush timer so the whole batch waits together
    if source_id in _flush_tasks:
        _flush_tasks[source_id].cancel()
    _flush_tasks[source_id] = asyncio.create_task(_flush(source_id))


# ── Startup helpers ───────────────────────────────────────────────────────────

async def start_af_queue(bot_client: Client):
    """Start the queue worker. Call once after bot.start()."""
    global _queue_running
    if _queue_running:
        return
    _queue_running = True
    asyncio.create_task(_queue_worker())
    logger.info("[af_engine] queue worker started")


async def start_all_userbot_af():
    """
    Called at bot startup.
    For every user who has an AF config with sources+targets AND a saved
    userbot session, start one Pyrogram client to monitor their sources.
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
            logger.error(f"[af_engine] Could not start userbot for user {uid}: {e}")
    logger.info(f"[af_engine] {started} userbot AF client(s) started at boot")


async def start_userbot_af(user_id: int, session_string: str) -> None:
    """
    Start (or restart) the userbot Pyrogram client for user_id.
    The client registers its own MessageHandler for channel media.
    Filters and speed are fetched from the user's AF config per message.
    """
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
            source_id = message.chat.id
            cfg       = await db.get_af_config(user_id)

            # Check source membership
            if not any(s["id"] == source_id for s in cfg.get("sources", [])):
                return

            target_ids = [t["id"] for t in cfg.get("targets", [])]
            if not target_ids:
                return

            # Apply filters
            if not _passes_af_filter(message, cfg):
                logger.debug(f"[af_engine] msg {message.id} filtered out for user {user_id}")
                return

            speed = cfg.get("speed", AF_DEFAULT_SPEED)
            _add_to_buffer(source_id, message, target_ids, ub_client,
                           is_userbot=True, speed=speed)
        except Exception as e:
            logger.error(f"[af_engine] userbot handler error (uid {user_id}): {e}")

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
# Only forwards for users whose userbot is NOT running — avoids duplication.

@Client.on_message(filters.channel & _MEDIA)
async def _bot_channel_handler(bot_client: Client, message):
    try:
        source_id = message.chat.id

        # get_source_users → [(user_id, [target_ids], full_cfg_doc), ...]
        user_entries = await db.get_source_users(source_id)
        if not user_entries:
            return

        for uid, tids, cfg in user_entries:
            # Skip users whose userbot is already handling this source
            if uid in _running_userbots:
                continue

            # Apply per-user filters
            if not _passes_af_filter(message, cfg):
                logger.debug(
                    f"[af_engine] bot: msg {message.id} filtered out for user {uid}"
                )
                continue

            speed = cfg.get("speed", AF_DEFAULT_SPEED)
            _add_to_buffer(
                source_id, message, tids,
                bot_client, is_userbot=False, speed=speed,
            )

    except Exception as e:
        logger.error(f"[af_engine] bot handler error: {e}", exc_info=True)
