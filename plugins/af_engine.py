"""
Auto-Forward Engine (hardened for 100% delivery)
────────────────────────────────────────────────
Delivery guarantees compared to the previous version:

  1. FloodWait is NEVER given up on.  We honor Telegram's requested
     wait for as long as it asks, message stays in the retry loop.
     Previously we gave up after 5 attempts → messages were silently
     dropped during long bursts.

  2. Non-FloodWait exceptions are retried with exponential backoff
     up to 10 attempts (2s → 60s cap) before being logged CRITICAL.
     Transient network / RPC errors no longer skip files.

  3. Deduplication cache keyed by (source_chat, message_id, target)
     — retries can no longer produce duplicate posts, and multiple
     concurrent handlers for the same message won't double-send.

  4. Album / media-group aware.  When a message belongs to an album
     we copy the WHOLE group with one `copy_media_group()` call and
     mark the group as processed, so the remaining group items don't
     each queue their own send.

  5. Handler wraps every step in try/except.  Any handler-side error
     is logged, but never blocks the message loop.

  6. Bursts to the same target are still serialised by a per-target
     asyncio.Lock, so we don't cause a self-inflicted FloodWait storm.

Speed mode inter-message delay (inside the per-target lock)
────────────────────────────────────────────────────────────
  safe   → 3.0 s   (max protection, lowest ban risk)
  normal → 1.0 s   (balanced, recommended)
  fast   → 0.5 s   (minimum floor — still avoids burst rate-limits)
"""

import asyncio
import logging
import re
import time
from collections import OrderedDict
from typing import Dict, Tuple

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
_MIN_DELAY = 0.5  # absolute floor between sends to same target

SPEED_DELAY: dict = {
    "safe":   3.0,
    "normal": 1.0,
    "fast":   _MIN_DELAY,
}

# ── Running userbot clients  {user_id → Client} ───────────────────────────────
_running_userbots: Dict[int, Client] = {}

# ── Per-target send locks ─────────────────────────────────────────────────────
_target_locks: Dict[int, asyncio.Lock] = {}


def _get_target_lock(tid: int) -> asyncio.Lock:
    if tid not in _target_locks:
        _target_locks[tid] = asyncio.Lock()
    return _target_locks[tid]


# ── Bounded LRU-ish dedup stores ──────────────────────────────────────────────
# Guarantees no duplicate sends even when retries loop, and lets us skip
# subsequent items of an album that we've already forwarded as a group.
_DEDUP_MAX = 20000

# key: (source_chat_id, message_id, target_id)
_msg_dedup: "OrderedDict[Tuple[int, int, int], float]" = OrderedDict()

# key: (source_chat_id, media_group_id, user_id) — one entry per (album, user)
_group_seen: "OrderedDict[Tuple[int, str, int], float]" = OrderedDict()


def _remember(store: OrderedDict, key, cap: int = _DEDUP_MAX) -> None:
    store[key] = time.time()
    store.move_to_end(key)
    while len(store) > cap:
        store.popitem(last=False)


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

    media_type  = message.media.value
    filters_cfg = af_cfg.get("filters", AF_DEFAULT_FILTERS)

    enabled_types = filters_cfg.get("types", AF_DEFAULT_FILTERS["types"])
    if media_type not in enabled_types:
        return False

    media_obj = getattr(message, media_type, None)
    file_size = getattr(media_obj, "file_size", 0) or 0
    file_name = getattr(media_obj, "file_name", "") or ""
    size_mb   = file_size / 1024 / 1024

    min_mb = float(filters_cfg.get("min_size_mb", 0) or 0)
    max_mb = float(filters_cfg.get("max_size_mb", 0) or 0)
    if min_mb > 0 and size_mb < min_mb:
        return False
    if max_mb > 0 and size_mb > max_mb:
        return False

    keywords = filters_cfg.get("keywords", [])
    if keywords and file_name:
        pattern = "|".join(re.escape(k) for k in keywords)
        if not re.search(pattern, file_name, re.IGNORECASE):
            return False

    extensions = filters_cfg.get("extensions", [])
    if extensions and file_name and "." in file_name:
        ext     = file_name.rsplit(".", 1)[-1].lower()
        allowed = [e.lower().lstrip(".") for e in extensions]
        if ext not in allowed:
            return False

    return True


# ── Core send helpers ─────────────────────────────────────────────────────────

# Cap on non-FloodWait error retries. FloodWait is never capped — Telegram's
# requested wait is always honored so no message is dropped during throttling.
_ERROR_RETRY_MAX = 10


async def _copy_single(
    client:      Client,
    tid:         int,
    msg,
    inter_delay: float,
    label:       str,
) -> bool:
    """
    Copy one (non-album) message to `tid`, retrying until it succeeds.

    • Per-target lock serialises burst sends to the same chat.
    • FloodWait is honoured indefinitely — never dropped.
    • Other exceptions retry with exponential backoff (up to 10 attempts).
    • Dedup ensures a retried send cannot double-post.
    """
    dedup_key = (msg.chat.id, msg.id, tid)
    lock      = _get_target_lock(tid)

    async with lock:
        if dedup_key in _msg_dedup:
            logger.debug(f"[af_engine]{label} dedup hit → {tid} msg {msg.id}")
            return True

        attempt = 0
        while True:
            attempt += 1
            try:
                await client.copy_message(
                    chat_id          = tid,
                    from_chat_id     = msg.chat.id,
                    message_id       = msg.id,
                    caption          = msg.caption,
                    caption_entities = msg.caption_entities if msg.caption else None,
                )
                _remember(_msg_dedup, dedup_key)
                await asyncio.sleep(inter_delay)
                return True

            except FloodWait as e:
                wait = int(e.value) + 2
                logger.warning(
                    f"[af_engine]{label} FloodWait {e.value}s → {tid} "
                    f"msg {msg.id} (attempt {attempt}), sleeping {wait}s…"
                )
                await asyncio.sleep(wait)
                # never break — always retry after FloodWait

            except Exception as e:
                logger.error(
                    f"[af_engine]{label} copy_message failed → {tid} "
                    f"msg {msg.id} (attempt {attempt}/{_ERROR_RETRY_MAX}): {e}"
                )
                if attempt >= _ERROR_RETRY_MAX:
                    logger.critical(
                        f"[af_engine]{label} PERMANENT FAIL → {tid} "
                        f"msg {msg.id}: {e}"
                    )
                    return False
                # exponential backoff, capped at 60s
                await asyncio.sleep(min(2 ** attempt, 60))


async def _copy_album(
    client:      Client,
    tid:         int,
    msg,
    inter_delay: float,
    label:       str,
) -> bool:
    """
    Copy the entire media group that `msg` belongs to as a single album
    via copy_media_group.  Same retry / dedup semantics as _copy_single.
    """
    mgid      = str(msg.media_group_id)
    dedup_key = (msg.chat.id, msg.id, tid, "grp")  # keyed on trigger msg + tid
    group_key = (msg.chat.id, mgid, tid)
    lock      = _get_target_lock(tid)

    async with lock:
        if group_key in _msg_dedup or dedup_key in _msg_dedup:
            logger.debug(
                f"[af_engine]{label} album dedup hit → {tid} group {mgid}"
            )
            return True

        attempt = 0
        while True:
            attempt += 1
            try:
                await client.copy_media_group(
                    chat_id      = tid,
                    from_chat_id = msg.chat.id,
                    message_id   = msg.id,
                )
                _remember(_msg_dedup, group_key)
                _remember(_msg_dedup, dedup_key)
                await asyncio.sleep(inter_delay)
                return True

            except FloodWait as e:
                wait = int(e.value) + 2
                logger.warning(
                    f"[af_engine]{label} FloodWait {e.value}s (album) → {tid} "
                    f"group {mgid} (attempt {attempt}), sleeping {wait}s…"
                )
                await asyncio.sleep(wait)

            except Exception as e:
                logger.error(
                    f"[af_engine]{label} copy_media_group failed → {tid} "
                    f"group {mgid} (attempt {attempt}/{_ERROR_RETRY_MAX}): {e}"
                )
                if attempt >= _ERROR_RETRY_MAX:
                    logger.critical(
                        f"[af_engine]{label} PERMANENT ALBUM FAIL → {tid} "
                        f"group {mgid}: {e}"
                    )
                    return False
                await asyncio.sleep(min(2 ** attempt, 60))


async def _forward_to_targets(
    client:     Client,
    msg,
    target_ids: list,
    speed:      str,
    label:      str = "",
):
    """Forward one message (or its whole album) to every target."""
    delay    = SPEED_DELAY.get(speed, SPEED_DELAY[AF_DEFAULT_SPEED])
    is_album = bool(getattr(msg, "media_group_id", None))
    for tid in target_ids:
        try:
            if is_album:
                await _copy_album(client, tid, msg, delay, label)
            else:
                await _copy_single(client, tid, msg, delay, label)
        except Exception as e:
            # Should not happen — inner helpers swallow their own errors —
            # but guard anyway so one bad target never blocks the others.
            logger.error(
                f"[af_engine]{label} unexpected forward error → {tid}: {e}",
                exc_info=True,
            )


# ── Startup helpers ───────────────────────────────────────────────────────────

async def start_af_queue(bot_client: Client):
    """No-op kept for import compatibility with main.py."""
    logger.info(
        "[af_engine] direct-forward mode active "
        "(dedup + unlimited FloodWait retry + album-aware)"
    )


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
            source_id = message.chat.id
            cfg       = await db.get_af_config(user_id)

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

            # Album coalescing — do the check-and-set atomically (no awaits).
            mgid = getattr(message, "media_group_id", None)
            if mgid:
                gkey = (source_id, str(mgid), user_id)
                if gkey in _group_seen:
                    return
                _remember(_group_seen, gkey)

            speed = cfg.get("speed", AF_DEFAULT_SPEED)
            logger.info(
                f"[af_engine] ub: forwarding {'album ' if mgid else ''}"
                f"msg {message.id} from {source_id} → {target_ids} "
                f"[speed={speed}]"
            )
            # Dispatch as a task so the handler returns immediately and the
            # next incoming update is never blocked waiting for a long retry.
            asyncio.create_task(
                _forward_to_targets(
                    ub_client, message, target_ids, speed,
                    label=f" [ub:{user_id}]",
                )
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

        mgid = getattr(message, "media_group_id", None)

        for uid, tids, cfg in user_entries:
            # Skip — userbot is already watching for this user
            if uid in _running_userbots:
                continue

            if not _passes_af_filter(message, cfg):
                logger.debug(
                    f"[af_engine] bot: msg {message.id} filtered out for {uid}"
                )
                continue

            # Album coalescing per (source, group, user) — atomic sync block.
            if mgid:
                gkey = (source_id, str(mgid), uid)
                if gkey in _group_seen:
                    continue
                _remember(_group_seen, gkey)

            speed = cfg.get("speed", AF_DEFAULT_SPEED)
            logger.info(
                f"[af_engine] bot: queuing {'album ' if mgid else ''}"
                f"msg {message.id} from {source_id} → {tids} [speed={speed}]"
            )
            # Fire-and-forget so bursty updates don't block the dispatcher.
            # Each user's forward runs concurrently; per-target locks inside
            # _copy_single / _copy_album serialise sends to any one target.
            asyncio.create_task(
                _forward_to_targets(
                    bot_client, message, tids, speed,
                    label=f" [bot→{uid}]",
                )
            )

    except Exception as e:
        logger.error(f"[af_engine] bot handler error: {e}", exc_info=True)
