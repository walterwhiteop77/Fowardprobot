"""
Auto-Forward Engine (reliable many-to-many delivery)
────────────────────────────────────────────────────

Fixes included here:
  • All enabled media types are watched from channels and groups.
  • Every matching source is copied to every target; targets run independently.
  • Userbot and bot-side listeners can both try the same message.  Dedup happens
    only after a successful target copy, so a failed listener cannot suppress the
    other one and lose files.
  • FloodWait and transient RPC/network errors are retried forever.  Only fatal
    permission/message errors stop retrying for that one target.
  • Albums are copied as one media group and deduped per source/group/target.
  • Fire-and-forget tasks are tracked so crashes are logged instead of silent.
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

_LEGACY_DEFAULT_TYPES = {"video", "document", "photo", "audio", "voice", "animation"}


# ── Media/source filters ──────────────────────────────────────────────────────

_MEDIA = (
    filters.video
    | filters.document
    | filters.photo
    | filters.audio
    | filters.voice
    | filters.animation
    | filters.video_note
    | filters.sticker
)

# Watch channels and groups/supergroups.  Some source chats are added as
# supergroups, not plain broadcast channels; filtering only channels misses them.
_SOURCE_MESSAGES = (filters.channel | filters.group) & _MEDIA


# ── Inter-message delay inside the per-target lock ────────────────────────────

_MIN_DELAY = 0.5

SPEED_DELAY: dict = {
    "safe": 3.0,
    "normal": 1.0,
    "fast": _MIN_DELAY,
}


# ── Running userbot clients {user_id → Client} ────────────────────────────────

_running_userbots: Dict[int, Client] = {}


# ── Per-target send locks ─────────────────────────────────────────────────────

_target_locks: Dict[int, asyncio.Lock] = {}


def _get_target_lock(tid: int) -> asyncio.Lock:
    if tid not in _target_locks:
        _target_locks[tid] = asyncio.Lock()
    return _target_locks[tid]


# ── Bounded dedup stores ──────────────────────────────────────────────────────

_DEDUP_MAX = 50000

# Single message key: (source_chat_id, message_id, target_id)
_msg_dedup: "OrderedDict[Tuple[int, int, int], float]" = OrderedDict()

# Album key: (source_chat_id, media_group_id, target_id, "album")
_album_dedup: "OrderedDict[Tuple[int, str, int, str], float]" = OrderedDict()

# Kept for import/backward compatibility with the previous build.  The engine no
# longer suppresses handlers before successful send; doing so was one cause of
# missing album/files from some sources.
_group_seen: "OrderedDict[Tuple[int, str, int], float]" = OrderedDict()


def _remember(store: OrderedDict, key, cap: int = _DEDUP_MAX) -> None:
    store[key] = time.time()
    store.move_to_end(key)
    while len(store) > cap:
        store.popitem(last=False)


# ── Filter helper ─────────────────────────────────────────────────────────────

def _passes_af_filter(message, af_cfg: dict) -> bool:
    """Return True if the message satisfies all active auto-forward filters."""
    if not getattr(message, "media", None):
        return False

    media_type = message.media.value
    filters_cfg = af_cfg.get("filters") or AF_DEFAULT_FILTERS

    enabled_types = list(filters_cfg.get("types") or AF_DEFAULT_FILTERS["types"])
    # Existing MongoDB configs created by older builds only contain the legacy
    # six media types. Treat that as "all default media" so stickers/video notes
    # are not silently skipped after upgrading.
    if set(enabled_types) == _LEGACY_DEFAULT_TYPES:
        enabled_types = list(AF_DEFAULT_FILTERS["types"])
    if media_type not in enabled_types:
        return False

    media_obj = getattr(message, media_type, None)
    file_size = getattr(media_obj, "file_size", 0) or 0
    file_name = getattr(media_obj, "file_name", "") or ""
    size_mb = file_size / 1024 / 1024

    min_mb = float(filters_cfg.get("min_size_mb", 0) or 0)
    max_mb = float(filters_cfg.get("max_size_mb", 0) or 0)
    if min_mb > 0 and size_mb < min_mb:
        return False
    if max_mb > 0 and size_mb > max_mb:
        return False

    # Filename-only filters should not accidentally drop media with no filename
    # (photos, voice, video notes, stickers).  They only apply when a filename is
    # available to inspect.
    keywords = filters_cfg.get("keywords", []) or []
    if keywords and file_name:
        pattern = "|".join(re.escape(k) for k in keywords)
        if not re.search(pattern, file_name, re.IGNORECASE):
            return False

    extensions = filters_cfg.get("extensions", []) or []
    if extensions and file_name and "." in file_name:
        ext = file_name.rsplit(".", 1)[-1].lower()
        allowed = [e.lower().lstrip(".") for e in extensions]
        if ext not in allowed:
            return False

    return True


# ── Retry helpers ─────────────────────────────────────────────────────────────

_FATAL_COPY_ERRORS = (
    "CHAT_ADMIN_REQUIRED",
    "CHAT_WRITE_FORBIDDEN",
    "USER_BANNED_IN_CHANNEL",
    "CHANNEL_PRIVATE",
    "PEER_ID_INVALID",
    "MESSAGE_ID_INVALID",
    "MSG_ID_INVALID",
    "MESSAGE_EMPTY",
    "MEDIA_EMPTY",
    "FILE_REFERENCE_EMPTY",
    "BOT_METHOD_INVALID",
)


def _is_fatal_copy_error(exc: Exception) -> bool:
    text = f"{type(exc).__name__}: {exc}".upper()
    return any(marker in text for marker in _FATAL_COPY_ERRORS)


def _track_task(task: asyncio.Task, label: str) -> None:
    """Make background task failures visible in bot.log/stdout."""

    def _done(done: asyncio.Task):
        try:
            done.result()
        except asyncio.CancelledError:
            logger.warning(f"[af_engine]{label} task cancelled")
        except Exception as e:
            logger.error(f"[af_engine]{label} task crashed: {e}", exc_info=True)

    task.add_done_callback(_done)


# ── Core send helpers ─────────────────────────────────────────────────────────

async def _copy_single(
    client: Client,
    tid: int,
    msg,
    inter_delay: float,
    label: str,
) -> bool:
    """Copy one non-album message to one target with persistent retries."""
    dedup_key = (msg.chat.id, msg.id, tid)
    lock = _get_target_lock(tid)

    async with lock:
        if dedup_key in _msg_dedup:
            logger.debug(f"[af_engine]{label} dedup hit → {tid} msg {msg.id}")
            return True

        attempt = 0
        while True:
            attempt += 1
            try:
                await client.copy_message(
                    chat_id=tid,
                    from_chat_id=msg.chat.id,
                    message_id=msg.id,
                    caption=msg.caption,
                    caption_entities=msg.caption_entities if msg.caption else None,
                )
                _remember(_msg_dedup, dedup_key)
                await asyncio.sleep(inter_delay)
                return True

            except FloodWait as e:
                wait = int(e.value) + 2
                logger.warning(
                    f"[af_engine]{label} FloodWait {e.value}s → {tid} "
                    f"msg {msg.id} attempt {attempt}; sleeping {wait}s"
                )
                await asyncio.sleep(wait)

            except Exception as e:
                if _is_fatal_copy_error(e):
                    logger.critical(
                        f"[af_engine]{label} fatal copy failure → {tid} "
                        f"msg {msg.id}: {e}"
                    )
                    return False

                wait = min(2 ** min(attempt, 6), 60)
                logger.error(
                    f"[af_engine]{label} transient copy failure → {tid} "
                    f"msg {msg.id} attempt {attempt}: {e}; retrying in {wait}s"
                )
                await asyncio.sleep(wait)


async def _copy_album(
    client: Client,
    tid: int,
    msg,
    inter_delay: float,
    label: str,
) -> bool:
    """Copy a whole media group to one target with persistent retries."""
    mgid = str(msg.media_group_id)
    group_key = (msg.chat.id, mgid, tid, "album")
    lock = _get_target_lock(tid)

    async with lock:
        if group_key in _album_dedup:
            logger.debug(f"[af_engine]{label} album dedup hit → {tid} group {mgid}")
            return True

        attempt = 0
        while True:
            attempt += 1
            try:
                await client.copy_media_group(
                    chat_id=tid,
                    from_chat_id=msg.chat.id,
                    message_id=msg.id,
                )
                _remember(_album_dedup, group_key)
                await asyncio.sleep(inter_delay)
                return True

            except FloodWait as e:
                wait = int(e.value) + 2
                logger.warning(
                    f"[af_engine]{label} FloodWait {e.value}s album → {tid} "
                    f"group {mgid} attempt {attempt}; sleeping {wait}s"
                )
                await asyncio.sleep(wait)

            except Exception as e:
                if _is_fatal_copy_error(e):
                    logger.critical(
                        f"[af_engine]{label} fatal album failure → {tid} "
                        f"group {mgid}: {e}"
                    )
                    return False

                wait = min(2 ** min(attempt, 6), 60)
                logger.error(
                    f"[af_engine]{label} transient album failure → {tid} "
                    f"group {mgid} attempt {attempt}: {e}; retrying in {wait}s"
                )
                await asyncio.sleep(wait)


async def _forward_to_targets(
    client: Client,
    msg,
    target_ids: list,
    speed: str,
    label: str = "",
):
    """Forward one message/album to every target independently."""
    delay = SPEED_DELAY.get(speed, SPEED_DELAY[AF_DEFAULT_SPEED])
    is_album = bool(getattr(msg, "media_group_id", None))

    tasks = []
    for tid in list(dict.fromkeys(int(t) for t in target_ids)):
        if is_album:
            tasks.append(asyncio.create_task(_copy_album(client, tid, msg, delay, label)))
        else:
            tasks.append(asyncio.create_task(_copy_single(client, tid, msg, delay, label)))

    if tasks:
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for result in results:
            if isinstance(result, Exception):
                logger.error(f"[af_engine]{label} target task error: {result}", exc_info=True)


# ── Startup helpers ───────────────────────────────────────────────────────────

async def start_af_queue(bot_client: Client):
    """No-op kept for import compatibility with main.py."""
    logger.info(
        "[af_engine] reliable direct-forward mode active "
        "(all sources → all targets, unlimited transient retries)"
    )


async def start_all_userbot_af():
    """Start userbot AF clients for saved sessions at boot."""
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
            logger.error(f"[af_engine] could not start userbot for {uid}: {e}", exc_info=True)
    logger.info(f"[af_engine] {started} userbot AF client(s) started at boot")


async def start_userbot_af(user_id: int, session_string: str) -> None:
    """Start or restart the userbot listener for one owner."""
    await stop_userbot_af(user_id)

    ub = Client(
        f"af_ub_{user_id}",
        api_id=Config.API_ID,
        api_hash=Config.API_HASH,
        session_string=session_string,
        in_memory=True,
    )

    async def _handler(ub_client: Client, message):
        try:
            source_id = message.chat.id
            cfg = await db.get_af_config(user_id)

            if not any(int(s["id"]) == int(source_id) for s in cfg.get("sources", [])):
                return

            target_ids = [int(t["id"]) for t in cfg.get("targets", [])]
            if not target_ids:
                return

            if not _passes_af_filter(message, cfg):
                logger.debug(f"[af_engine] ub: msg {message.id} filtered out for {user_id}")
                return

            mgid = getattr(message, "media_group_id", None)
            speed = cfg.get("speed", AF_DEFAULT_SPEED)
            logger.info(
                f"[af_engine] ub: queuing {'album ' if mgid else ''}"
                f"msg {message.id} from {source_id} → {target_ids} [speed={speed}]"
            )

            task = asyncio.create_task(
                _forward_to_targets(
                    ub_client,
                    message,
                    target_ids,
                    speed,
                    label=f" [ub:{user_id}]",
                )
            )
            _track_task(task, f" [ub:{user_id}] msg {message.id}")

        except Exception as e:
            logger.error(f"[af_engine] userbot handler error uid {user_id}: {e}", exc_info=True)

    ub.add_handler(MessageHandler(_handler, _SOURCE_MESSAGES))
    await ub.start()
    me = await ub.get_me()
    _running_userbots[user_id] = ub
    logger.info(
        f"[af_engine] userbot AF ready — user {user_id}, account @{me.username or me.id}"
    )


async def stop_userbot_af(user_id: int) -> None:
    """Stop and remove the userbot client for user_id if running."""
    ub = _running_userbots.pop(user_id, None)
    if ub:
        try:
            await ub.stop()
        except Exception:
            pass
        logger.info(f"[af_engine] userbot AF stopped — user {user_id}")


# ── Bot-side source handler ───────────────────────────────────────────────────

@Client.on_message(_SOURCE_MESSAGES)
async def _bot_channel_handler(bot_client: Client, message):
    try:
        source_id = message.chat.id
        user_entries = await db.get_source_users(source_id)
        if not user_entries:
            return

        mgid = getattr(message, "media_group_id", None)
        for uid, tids, cfg in user_entries:
            # Do not skip when a userbot exists.  If either listener succeeds,
            # post-send dedup prevents duplicates; if one listener cannot access
            # a source/target, the other can still deliver the file.
            if not _passes_af_filter(message, cfg):
                logger.debug(f"[af_engine] bot: msg {message.id} filtered out for {uid}")
                continue

            speed = cfg.get("speed", AF_DEFAULT_SPEED)
            logger.info(
                f"[af_engine] bot: queuing {'album ' if mgid else ''}"
                f"msg {message.id} from {source_id} → {tids} [speed={speed}]"
            )

            task = asyncio.create_task(
                _forward_to_targets(
                    bot_client,
                    message,
                    tids,
                    speed,
                    label=f" [bot→{uid}]",
                )
            )
            _track_task(task, f" [bot→{uid}] msg {message.id}")

    except Exception as e:
        logger.error(f"[af_engine] bot handler error: {e}", exc_info=True)