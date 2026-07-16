"""
Auto-Forward Engine
───────────────────
Two forwarding paths share one async queue:

  BOT path     — handles source channels where the main bot is admin.
                 Active only for users who have NOT set up a userbot.

  USERBOT path — one Pyrogram Client per user who has a stored userbot
                 session.  Handles ALL source channels for that user,
                 including private ones the bot cannot read.

Rate limiting is fully delegated to plugins/rate_limiter.py:
  • Per-channel token bucket   (max 12 msgs/min per target channel)
  • Per-client global bucket   (max 15 msgs/s for bot, 4/s for userbot)
  • Circuit-breaker per target (pauses a broken target for 10 min)
  • FloodWait drain            (respects exact Telegram wait + jitter)
  • PeerFlood cooldown         (1-hour freeze when account is flagged)
"""

import asyncio
import logging
from collections import defaultdict
from typing import Dict, List, Tuple

from pyrogram import Client, filters
from pyrogram.handlers import MessageHandler

from config import Config
from database import db
from plugins.rate_limiter import safe_copy_message

logger = logging.getLogger(__name__)

# ── Media filter ──────────────────────────────────────────────────────────────
_MEDIA = (
    filters.video
    | filters.document
    | filters.photo
    | filters.audio
    | filters.voice
)

# ── Running userbot clients  {user_id → Client} ───────────────────────────────
# Exposed so autoforward.py and main.py can read it.
_running_userbots: Dict[int, Client] = {}

# ── Message buffer  {source_chat_id → [(Message, [target_ids], Client)]} ──────
_buffers: Dict[int, List[Tuple]] = defaultdict(list)
_flush_tasks: Dict[int, asyncio.Task] = {}

BUFFER_DELAY = 4   # seconds — lets media-group frames arrive before forwarding

# ── Forward queue  items: ([Message], [int target_ids], Client, bool is_ub) ───
_queue: asyncio.Queue = asyncio.Queue()
_queue_running = False


# ── Queue worker ──────────────────────────────────────────────────────────────

async def _queue_worker():
    """
    Consume forwarding jobs one by one.
    Each job: (messages, target_ids, copy_client, is_userbot).
    Calls safe_copy_message() for every (message, target) pair.
    safe_copy_message handles all FloodWait / rate-limiting internally.
    """
    while True:
        try:
            messages, target_ids, copy_client, is_ub = await _queue.get()

            for msg in sorted(messages, key=lambda m: m.id):
                for tid in target_ids:
                    await safe_copy_message(
                        copy_client,
                        chat_id      = tid,
                        from_chat_id = msg.chat.id,
                        message_id   = msg.id,
                        caption          = msg.caption,
                        caption_entities = msg.caption_entities if msg.caption else None,
                        is_userbot   = is_ub,
                    )

            _queue.task_done()

        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"[af_engine] queue worker error: {e}", exc_info=True)
            await asyncio.sleep(1)


# ── Buffer helpers ────────────────────────────────────────────────────────────

async def _flush(source_id: int):
    """Wait BUFFER_DELAY, then push the accumulated batch onto the queue."""
    await asyncio.sleep(BUFFER_DELAY)

    items = _buffers.pop(source_id, [])
    _flush_tasks.pop(source_id, None)
    if not items:
        return

    # Group items that share the same (frozenset of targets, client identity)
    # so we don't send the same message to the same target twice.
    groups: dict = {}
    for msg, target_ids, copy_client, is_ub in items:
        key = (frozenset(target_ids), id(copy_client))
        if key not in groups:
            groups[key] = ([], list(target_ids), copy_client, is_ub)
        groups[key][0].append(msg)

    for msgs, tids, cli, is_ub in groups.values():
        await _queue.put((msgs, tids, cli, is_ub))
        logger.info(
            f"[af_engine] queued {len(msgs)} msg(s) "
            f"from {source_id} → {len(tids)} target(s)"
        )


def _add_to_buffer(
    source_id:   int,
    message,
    target_ids:  List[int],
    copy_client: Client,
    is_userbot:  bool,
):
    _buffers[source_id].append((message, target_ids, copy_client, is_userbot))
    # Reset the flush timer so the whole batch waits together
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
    """
    await stop_userbot_af(user_id)   # tear down old client if running

    ub = Client(
        f"af_ub_{user_id}",
        api_id       = Config.API_ID,
        api_hash     = Config.API_HASH,
        session_string = session_string,
        in_memory    = True,
    )

    async def _handler(ub_client: Client, message):
        try:
            source_id = message.chat.id
            cfg = await db.get_af_config(user_id)
            if not any(s["id"] == source_id for s in cfg.get("sources", [])):
                return
            target_ids = [t["id"] for t in cfg.get("targets", [])]
            if not target_ids:
                return
            _add_to_buffer(source_id, message, target_ids, ub_client, is_userbot=True)
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

        # get_source_users → [(user_id, [target_ids]), ...]
        user_targets = await db.get_source_users(source_id)
        if not user_targets:
            return

        # Collect targets only from users whose userbot is NOT running
        bot_targets: List[int] = []
        seen: set = set()
        for uid, tids in user_targets:
            if uid in _running_userbots:
                continue     # userbot handles this user's targets
            for tid in tids:
                if tid not in seen:
                    seen.add(tid)
                    bot_targets.append(tid)

        if bot_targets:
            _add_to_buffer(
                source_id, message, bot_targets,
                bot_client, is_userbot=False
            )

    except Exception as e:
        logger.error(f"[af_engine] bot handler error: {e}", exc_info=True)
