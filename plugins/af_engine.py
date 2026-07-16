"""
Auto-Forward Engine
───────────────────
Listens to every channel message the bot receives.
If the channel is a registered source, buffers messages for BUFFER_DELAY
seconds (to catch media groups), then copies them to every mapped target.
"""

import asyncio
import logging
from collections import defaultdict
from pyrogram import Client, filters
from pyrogram.errors import FloodWait, RPCError
from database import db

logger = logging.getLogger(__name__)

BUFFER_DELAY = 4          # seconds to wait before flushing a buffer batch

# ── Per-source message buffers & pending tasks ────────────────────────────────
message_buffer: dict = defaultdict(list)
buffer_tasks:   dict = {}

# ── Single async queue consumed by one worker per bot session ─────────────────
forward_queue = asyncio.Queue()
_processor_started = False


# ─── Queue worker ─────────────────────────────────────────────────────────────

async def _queue_worker(client):
    """Consume (messages, target_ids) tuples and forward one-by-one."""
    while True:
        try:
            messages, target_ids = await forward_queue.get()

            sorted_msgs = sorted(messages, key=lambda m: m.id)

            for target_id in target_ids:
                for msg in sorted_msgs:
                    await _copy_with_retry(client, msg, target_id)
                    await asyncio.sleep(0.3)          # small gap between files
                await asyncio.sleep(0.5)              # gap between targets

            forward_queue.task_done()

        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"[af_engine] queue worker error: {e}")
            await asyncio.sleep(1)


async def _copy_with_retry(client, message, target_id, retries=3):
    for attempt in range(retries):
        try:
            kwargs = {
                "chat_id":     target_id,
                "from_chat_id": message.chat.id,
                "message_id":  message.id,
            }
            if message.caption:
                kwargs["caption"] = message.caption
                if message.caption_entities:
                    kwargs["caption_entities"] = message.caption_entities

            await client.copy_message(**kwargs)
            logger.info(
                f"[af_engine] copied msg {message.id} "
                f"from {message.chat.id} → {target_id}"
            )
            return

        except FloodWait as e:
            logger.warning(f"[af_engine] FloodWait {e.value}s → {target_id}")
            await asyncio.sleep(e.value + 1)

        except RPCError as e:
            logger.error(f"[af_engine] RPCError → {target_id}: {e}")
            if attempt < retries - 1:
                await asyncio.sleep(2 ** attempt)
            else:
                logger.error(f"[af_engine] giving up on msg {message.id} → {target_id}")

        except Exception as e:
            logger.error(f"[af_engine] unexpected error → {target_id}: {e}")
            return


# ─── Buffer flusher ───────────────────────────────────────────────────────────

async def _flush_buffer(source_chat_id, client):
    """Called after BUFFER_DELAY; looks up targets and enqueues the batch."""
    await asyncio.sleep(BUFFER_DELAY)

    messages = message_buffer.pop(source_chat_id, [])
    buffer_tasks.pop(source_chat_id, None)

    if not messages:
        return

    try:
        mappings = await db.get_af_all_targets_for_source(source_chat_id)
        if not mappings:
            return

        # Collect unique target IDs across all users who monitor this source
        all_targets = []
        seen = set()
        for mapping in mappings:
            for t in mapping.get("target_ids", []):
                t_id = t["id"]
                if t_id not in seen:
                    seen.add(t_id)
                    all_targets.append(t_id)

        if all_targets:
            await forward_queue.put((messages.copy(), all_targets))
            logger.info(
                f"[af_engine] queued {len(messages)} msg(s) "
                f"from {source_chat_id} → {len(all_targets)} target(s)"
            )

    except Exception as e:
        logger.error(f"[af_engine] flush error for {source_chat_id}: {e}")


# ─── Public startup helper ────────────────────────────────────────────────────

async def start_af_queue(client):
    """Call once after bot.start() to launch the queue worker."""
    global _processor_started
    if _processor_started:
        return
    _processor_started = True
    asyncio.create_task(_queue_worker(client))
    logger.info("[af_engine] auto-forward queue worker started")


# ─── Pyrogram handler: fires on every channel message ─────────────────────────

@Client.on_message(
    filters.channel
    & (
        filters.video
        | filters.document
        | filters.photo
        | filters.audio
        | filters.voice
    )
)
async def auto_forward_handler(client, message):
    """
    Triggered whenever the bot sees a media message in a channel it belongs to.
    We do a quick DB check to see if this channel is a tracked source before
    buffering anything.
    """
    try:
        source_id = message.chat.id

        # Fast path: is this channel tracked by anyone?
        mappings = await db.get_af_all_targets_for_source(source_id)
        if not mappings:
            return

        # Buffer the message
        message_buffer[source_id].append(message)

        # Reset (or start) the flush timer
        if source_id in buffer_tasks:
            buffer_tasks[source_id].cancel()

        buffer_tasks[source_id] = asyncio.create_task(
            _flush_buffer(source_id, client)
        )

    except Exception as e:
        logger.error(f"[af_engine] handler error: {e}", exc_info=True)
