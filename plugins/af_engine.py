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

FloodWait handling
──────────────────
Each send is protected by a per-target asyncio.Lock.  When 5-6 messages
arrive simultaneously they queue up per-target so only ONE copy_message
is in-flight to a given target at any time.  On FloodWait we sleep the
exact amount Telegram requests and retry once before giving up.

Speed mode extra delay
──────────────────────
  safe   → 3.0 s sleep after each copy_message call
  normal → 1.0 s
  fast   → 0.0 s  (no extra sleep)

This is purely an optional throttle the user controls via ⚡ Speed.
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

# ── Speed extra delay (optional, purely cosmetic throttle) ────────────────────
SPEED_EXTRA_DELAY: dict = {
    "safe":   3.0,
    "normal": 1.0,
    "fast":   0.0,
}

# ── Running userbot clients  {user_id → Client} ───────────────────────────────
_running_userbots: Dict[int, Client] = {}

# ── Per-target send locks ─────────────────────────────────────────────────────
# Serialises copy_message calls to the same target so that a burst of 5-6
# simultaneous messages doesn't cause concurrent FloodWait races where all
# tasks retry at the same instant and all get dropped.
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

async def _do_copy(client: Client, tid: int, msg, label: str = "") -> bool:
    """
    copy_message to a single target.

    Acquires a per-target lock first so that burst/album messages sent at the
    same time are serialised — only one copy_message call is in-flight to a
    given target at any moment.  On FloodWait, sleep the exact time Telegram
    requests and retry once before giving up.

    Returns True on success, False on failure.
    """
    lock = _get_target_lock(tid)
    async with lock:
        for attempt in range(2):
            try:
                await client.copy_message(
                    chat_id          = tid,
                    from_chat_id     = msg.chat.id,
                    message_id       = msg.id,
                    caption          = msg.caption,
                    caption_entities = msg.caption_entities if msg.caption else None,
                )
                return True
            except FloodWait as e:
                if attempt == 0:
                    logger.warning(
                        f"[af_engine]{label} FloodWait {e.value}s → {tid}, sleeping…"
                    )
                    await asyncio.sleep(e.value + 1)
                else:
                    logger.error(
                        f"[af_engine]{label} FloodWait again → {tid}, skipping"
                    )
                    return False
            except Exception as e:
                logger.error(f"[af_engine]{label} copy_message failed → {tid}: {e}")
                return False
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
    Applies the speed-mode extra delay between sends.
    The per-target lock inside _do_copy handles concurrent bursts automatically.
    """
    extra = SPEED_EXTRA_DELAY.get(speed, SPEED_EXTRA_DELAY[AF_DEFAULT_SPEED])
    for idx, tid in enumerate(target_ids):
        await _do_copy(client, tid, msg, label)
        if extra > 0 and idx < len(target_ids) - 1:
            await asyncio.sleep(extra)


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
                f"[af_engine] bot: forwarding msg {message.id} "
                f"from {source_id} → {tids} [speed={speed}]"
            )
            # Run each user's forward concurrently.
            # Per-target locks inside _do_copy ensure burst messages to the
            # same target are still serialised, so no concurrent FloodWaits.
            asyncio.create_task(
                _forward_to_targets(
                    bot_client, message, tids, speed,
                    label=f" [bot→{uid}]",
                )
            )

    except Exception as e:
        logger.error(f"[af_engine] bot handler error: {e}", exc_info=True)
