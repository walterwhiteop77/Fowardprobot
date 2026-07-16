"""
allowlist.py — Access-control system
=====================================

When allow-mode is ON, only the bot owner and explicitly whitelisted users
can interact with the bot. Everyone else gets a polite refusal.

How it works
────────────
Two group=-1 handlers run BEFORE all other handlers (which default to group=0):

  • _msg_gate       — intercepts every private message
  • _cbq_gate       — intercepts every callback query

If the user passes the check, the handlers return silently and the normal
handlers process the event as usual. If not, a refusal is sent and
stop_propagation() is called so no other handler ever sees the event.

Owner commands (BOT_OWNER only)
────────────────────────────────
  /allowmode on|off  — toggle the whitelist gate
  /adduser <id|reply>  — add a user to the whitelist
  /removeuser <id|reply>  — remove a user from the whitelist
  /allowedusers  — list all whitelisted user IDs
"""

import logging
from pyrogram import Client, filters
from pyrogram.types import Message, CallbackQuery
from pyrogram.handlers import MessageHandler, CallbackQueryHandler

from config import Config, temp
from database import db

logger = logging.getLogger(__name__)

# ── Internal helpers ──────────────────────────────────────────────────────────

async def _allow_mode() -> bool:
    """Return allow-mode state, loading from DB the first time."""
    if temp.ALLOW_MODE is None:
        temp.ALLOW_MODE = await db.get_allow_mode()
    return temp.ALLOW_MODE


async def _is_allowed(user_id: int) -> bool:
    """True if user may use the bot right now."""
    if user_id == Config.BOT_OWNER:
        return True
    if not await _allow_mode():
        return True                          # gate is off — everyone allowed
    return await db.is_whitelisted(user_id)


def _owner_only(_, __, message: Message) -> bool:
    return message.from_user and message.from_user.id == Config.BOT_OWNER


owner_filter = filters.create(_owner_only)


async def _resolve_target_id(message: Message) -> int | None:
    """
    Extract a user_id from:
      • a plain integer argument: /adduser 123456789
      • a reply to any message from that user
      • a reply to a forwarded message
    Returns None if nothing usable is found.
    """
    args = message.text.split()
    if len(args) > 1:
        try:
            return int(args[1])
        except ValueError:
            pass

    if message.reply_to_message:
        rm = message.reply_to_message
        if rm.from_user:
            return rm.from_user.id
        if rm.forward_from:
            return rm.forward_from.id

    return None


# ── Group -1 gates ────────────────────────────────────────────────────────────
# These run BEFORE any group-0 handler. Returning normally lets the event
# continue; calling stop_propagation() blocks it completely.

@Client.on_message(filters.private & filters.incoming, group=-1)
async def _msg_gate(client: Client, message: Message):
    user_id = message.from_user.id if message.from_user else None
    if user_id and not await _is_allowed(user_id):
        await message.reply(
            "⛔ <b>Access Denied</b>\n\n"
            "You are not authorised to use this bot.\n"
            "Contact the bot owner to request access."
        )
        message.stop_propagation()


@Client.on_callback_query(filters.private, group=-1)
async def _cbq_gate(client: Client, query: CallbackQuery):
    user_id = query.from_user.id if query.from_user else None
    if user_id and not await _is_allowed(user_id):
        await query.answer(
            "⛔ You are not authorised to use this bot.", show_alert=True
        )
        query.stop_propagation()


# ── Owner commands ────────────────────────────────────────────────────────────

@Client.on_message(filters.private & filters.command(["allowmode"]) & owner_filter)
async def cmd_allowmode(client: Client, message: Message):
    """
    /allowmode on   — enable whitelist gate
    /allowmode off  — disable whitelist gate (everyone can use the bot)
    """
    args = message.text.split()
    if len(args) < 2 or args[1].lower() not in ("on", "off"):
        current = await _allow_mode()
        state = "🟢 ON" if current else "🔴 OFF"
        return await message.reply(
            f"<b>Allow-Mode is currently {state}</b>\n\n"
            "Usage:\n"
            "  <code>/allowmode on</code>  — only whitelisted users\n"
            "  <code>/allowmode off</code> — everyone can use the bot"
        )

    enable = args[1].lower() == "on"
    await db.set_allow_mode(enable)
    temp.ALLOW_MODE = enable

    state = "🟢 ON" if enable else "🔴 OFF"
    msg = (
        f"✅ Allow-Mode turned <b>{state}</b>\n\n"
        + (
            "Only the bot owner and whitelisted users can now use the bot."
            if enable else
            "The bot is now open to all users."
        )
    )
    await message.reply(msg)
    logger.info(f"[allowlist] Allow-mode set to {enable} by owner")


@Client.on_message(filters.private & filters.command(["adduser"]) & owner_filter)
async def cmd_adduser(client: Client, message: Message):
    """
    /adduser 123456789       — add by user ID
    /adduser (reply)         — add the user whose message you replied to
    """
    uid = await _resolve_target_id(message)
    if uid is None:
        return await message.reply(
            "Usage: <code>/adduser &lt;user_id&gt;</code>  or reply to a message."
        )

    if uid == Config.BOT_OWNER:
        return await message.reply("ℹ️ The owner always has access — no need to whitelist.")

    added = await db.add_to_whitelist(uid)
    if added:
        await message.reply(f"✅ User <code>{uid}</code> added to the whitelist.")
        logger.info(f"[allowlist] User {uid} whitelisted by owner")
    else:
        await message.reply(f"ℹ️ User <code>{uid}</code> is already whitelisted.")


@Client.on_message(filters.private & filters.command(["removeuser"]) & owner_filter)
async def cmd_removeuser(client: Client, message: Message):
    """
    /removeuser 123456789    — remove by user ID
    /removeuser (reply)      — remove the user whose message you replied to
    """
    uid = await _resolve_target_id(message)
    if uid is None:
        return await message.reply(
            "Usage: <code>/removeuser &lt;user_id&gt;</code>  or reply to a message."
        )

    removed = await db.remove_from_whitelist(uid)
    if removed:
        await message.reply(f"✅ User <code>{uid}</code> removed from the whitelist.")
        logger.info(f"[allowlist] User {uid} removed from whitelist by owner")
    else:
        await message.reply(f"ℹ️ User <code>{uid}</code> was not on the whitelist.")


@Client.on_message(filters.private & filters.command(["allowedusers"]) & owner_filter)
async def cmd_allowedusers(client: Client, message: Message):
    """Show all whitelisted user IDs and current allow-mode state."""
    wl = await db.get_whitelist()
    mode = await _allow_mode()
    state = "🟢 ON" if mode else "🔴 OFF"

    if not wl:
        body = "<i>No users whitelisted yet.</i>"
    else:
        body = "\n".join(f"  • <code>{uid}</code>" for uid in wl)

    await message.reply(
        f"<b>Whitelist</b>  |  Allow-Mode: {state}\n\n"
        f"{body}\n\n"
        f"<i>Total: {len(wl)} user(s)</i>"
    )
