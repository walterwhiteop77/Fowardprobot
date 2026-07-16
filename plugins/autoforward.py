"""
Auto Forward Plugin — Interactive management UI
────────────────────────────────────────────────
/autoforward  →  main menu

Architecture
  • Sources  — flat list of channels to watch (public or private)
  • Targets  — flat list of channels to post into
  • Rule     — every file from ANY source → ALL targets
  • Userbot  — optional Pyrogram user-session for private-channel access
               (OTP login built into this menu)

Userbot requirement
  • Source channels  : userbot must be a member (not necessarily admin)
  • Target channels  : userbot must be admin (to post)
  When no userbot is configured the main bot handles forwarding
  (bot must be admin in source channels as well as targets).
"""

import asyncio
import logging

from pyrogram import Client, enums, filters
from pyrogram.errors import (
    ApiIdInvalid, PhoneNumberInvalid, PhoneCodeInvalid,
    PhoneCodeExpired, SessionPasswordNeeded, PasswordHashInvalid,
    FloodWait,
)
from pyrogram.types import (
    CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message,
)

from config import Config
from database import db

logger = logging.getLogger(__name__)

# ── Conversation state ────────────────────────────────────────────────────────
# {user_id: {
#   "step": str,
#   "phone_client": Client | None,   # live during OTP flow
#   "phone_number": str,
#   "phone_code_hash": str,
# }}
af_states: dict = {}

BLOCKED_CMDS = [
    "start", "forward", "settings", "stop", "reset", "restart",
    "resetall", "broadcast", "unequify", "autoforward",
]

# ── Keyboards ─────────────────────────────────────────────────────────────────

def _main_kb():
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("📥 Sources", callback_data="af:sources"),
            InlineKeyboardButton("📤 Targets", callback_data="af:targets"),
        ],
        [
            InlineKeyboardButton("👤 Userbot",  callback_data="af:userbot"),
            InlineKeyboardButton("📊 Status",   callback_data="af:status"),
        ],
        [InlineKeyboardButton("ℹ️ How It Works", callback_data="af:help")],
    ])


def _list_kb(items, remove_prefix, add_cb, back_cb="af:menu"):
    """Generic keyboard for source/target lists."""
    rows = []
    for it in items:
        label = it.get("title", str(it["id"]))[:28]
        rows.append([
            InlineKeyboardButton(f"• {label}", callback_data="af:noop"),
            InlineKeyboardButton(
                "❌", callback_data=f"{remove_prefix}:{it['id']}"
            ),
        ])
    rows.append([
        InlineKeyboardButton("➕ Add", callback_data=add_cb),
        InlineKeyboardButton("🔙 Back", callback_data=back_cb),
    ])
    return InlineKeyboardMarkup(rows)


def _cancel_kb():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("❌ Cancel", callback_data="af:cancel")]
    ])


def _back_kb(cb="af:menu"):
    return InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data=cb)]])


# ── /autoforward command ──────────────────────────────────────────────────────

@Client.on_message(filters.private & filters.command("autoforward"))
async def autoforward_cmd(client, message: Message):
    user = message.from_user
    if not await db.is_user_exist(user.id):
        await db.add_user(user.id, user.first_name)
    af_states.pop(user.id, None)
    await message.reply_text(
        "<b>🔄 Auto Forward Manager</b>\n\n"
        "Forward files from <b>source channels</b> to <b>target channels</b> "
        "automatically — the moment they are posted.\n\n"
        "<i>All sources → all targets  (many-to-many)</i>",
        reply_markup=_main_kb(),
    )


# ── Callback handler ──────────────────────────────────────────────────────────

@Client.on_callback_query(filters.regex(r"^af:"))
async def af_cb(client, query: CallbackQuery):
    uid  = query.from_user.id
    data = query.data

    # ── Menu ──────────────────────────────────────────────────────────────────
    if data == "af:menu":
        af_states.pop(uid, None)
        await _cancel_temp_client(uid)
        await query.message.edit_text(
            "<b>🔄 Auto Forward Manager</b>\n\n"
            "All sources → all targets  (many-to-many)",
            reply_markup=_main_kb(),
        )

    # ── Sources list ──────────────────────────────────────────────────────────
    elif data == "af:sources":
        af_states.pop(uid, None)
        cfg = await db.get_af_config(uid)
        srcs = cfg.get("sources", [])
        if srcs:
            lines = "\n".join(f"  • {s.get('title', s['id'])}" for s in srcs)
            text  = f"<b>📥 Source Channels ({len(srcs)})</b>\n\n{lines}\n\n" \
                    "Files from <b>any</b> of these are forwarded to <b>all</b> targets."
        else:
            text = "<b>📥 Source Channels</b>\n\nNo sources yet. Add one!"
        await query.message.edit_text(
            text,
            reply_markup=_list_kb(srcs, "af:dsrc", "af:addsrc", "af:menu"),
        )

    # ── Targets list ──────────────────────────────────────────────────────────
    elif data == "af:targets":
        af_states.pop(uid, None)
        cfg  = await db.get_af_config(uid)
        tgts = cfg.get("targets", [])
        if tgts:
            lines = "\n".join(f"  • {t.get('title', t['id'])}" for t in tgts)
            text  = f"<b>📤 Target Channels ({len(tgts)})</b>\n\n{lines}\n\n" \
                    "Files are copied into <b>all</b> of these when any source posts."
        else:
            text = "<b>📤 Target Channels</b>\n\nNo targets yet. Add one!"
        await query.message.edit_text(
            text,
            reply_markup=_list_kb(tgts, "af:dtgt", "af:addtgt", "af:menu"),
        )

    # ── Remove source ─────────────────────────────────────────────────────────
    elif data.startswith("af:dsrc:"):
        chat_id = int(data.split(":")[2])
        await db.remove_af_source(uid, chat_id)
        await query.answer("✅ Source removed!", show_alert=True)
        # Refresh list
        cfg  = await db.get_af_config(uid)
        srcs = cfg.get("sources", [])
        text = (
            f"<b>📥 Source Channels ({len(srcs)})</b>\n\n" +
            "\n".join(f"  • {s.get('title', s['id'])}" for s in srcs)
        ) if srcs else "<b>📥 Source Channels</b>\n\nNo sources yet."
        await query.message.edit_text(
            text,
            reply_markup=_list_kb(srcs, "af:dsrc", "af:addsrc", "af:menu"),
        )

    # ── Remove target ─────────────────────────────────────────────────────────
    elif data.startswith("af:dtgt:"):
        chat_id = int(data.split(":")[2])
        await db.remove_af_target(uid, chat_id)
        await query.answer("✅ Target removed!", show_alert=True)
        cfg  = await db.get_af_config(uid)
        tgts = cfg.get("targets", [])
        text = (
            f"<b>📤 Target Channels ({len(tgts)})</b>\n\n" +
            "\n".join(f"  • {t.get('title', t['id'])}" for t in tgts)
        ) if tgts else "<b>📤 Target Channels</b>\n\nNo targets yet."
        await query.message.edit_text(
            text,
            reply_markup=_list_kb(tgts, "af:dtgt", "af:addtgt", "af:menu"),
        )

    # ── Add source (start conversation) ───────────────────────────────────────
    elif data == "af:addsrc":
        af_states[uid] = {"step": "waiting_source"}
        await query.message.edit_text(
            "<b>➕ Add Source Channel</b>\n\n"
            "📥 <b>Forward any message</b> from the source channel here.\n"
            "Or send the channel's <b>@username</b> or <b>invite link</b>.\n\n"
            "<i>Userbot must be a member of private channels.\n"
            "Bot must be admin in public channels.</i>",
            reply_markup=_cancel_kb(),
        )

    # ── Add target (start conversation) ───────────────────────────────────────
    elif data == "af:addtgt":
        af_states[uid] = {"step": "waiting_target"}
        await query.message.edit_text(
            "<b>➕ Add Target Channel</b>\n\n"
            "📤 <b>Forward any message</b> from the target channel here.\n"
            "Or send the channel's <b>@username</b> or <b>invite link</b>.\n\n"
            "<i>Both the bot and userbot (if configured) must be <b>admin</b> "
            "in target channels.</i>",
            reply_markup=_cancel_kb(),
        )

    # ── Cancel conversation ───────────────────────────────────────────────────
    elif data == "af:cancel":
        await _cancel_temp_client(uid)
        af_states.pop(uid, None)
        await query.message.edit_text(
            "❌ <b>Cancelled.</b>",
            reply_markup=_back_kb("af:menu"),
        )

    # ── Status ────────────────────────────────────────────────────────────────
    elif data == "af:status":
        cfg = await db.get_af_config(uid)
        srcs = cfg.get("sources", [])
        tgts = cfg.get("targets", [])
        ub   = await db.get_userbot(uid)

        from plugins.af_engine import _running_userbots
        ub_status = "🟢 Running" if uid in _running_userbots else (
            "🔴 Not started" if ub else "⚪ Not configured"
        )

        src_lines = "\n".join(f"  📥 {s.get('title', s['id'])}" for s in srcs) or "  (none)"
        tgt_lines = "\n".join(f"  📤 {t.get('title', t['id'])}" for t in tgts) or "  (none)"

        await query.message.edit_text(
            f"<b>📊 Auto Forward Status</b>\n\n"
            f"<b>Sources ({len(srcs)}):</b>\n{src_lines}\n\n"
            f"<b>Targets ({len(tgts)}):</b>\n{tgt_lines}\n\n"
            f"<b>Userbot:</b> {ub_status}",
            reply_markup=_back_kb("af:menu"),
        )

    # ── Help ──────────────────────────────────────────────────────────────────
    elif data == "af:help":
        await query.message.edit_text(
            "<b>ℹ️ How Auto Forward Works</b>\n\n"
            "<b>1. Add Source Channels</b>\n"
            "   Forward a message from each source channel here.\n\n"
            "<b>2. Add Target Channels</b>\n"
            "   Forward a message from each target channel here.\n\n"
            "<b>3. Configure Userbot</b> (for private channels)\n"
            "   Go to 👤 Userbot and log in with your phone number.\n\n"
            "<b>4. Done!</b> Files from <b>any source</b> → <b>all targets</b> automatically.\n\n"
            "<b>Channel permissions:</b>\n"
            "  • Sources (public): bot must be admin\n"
            "  • Sources (private): userbot must be a member\n"
            "  • Targets: bot AND userbot must be admin\n\n"
            "Videos, documents, photos and audio are forwarded.\n"
            "No 'Forwarded from' tag — files appear as native posts.",
            reply_markup=_back_kb("af:menu"),
        )

    # ── No-op (decorative label buttons) ─────────────────────────────────────
    elif data == "af:noop":
        await query.answer()
        return

    # ── Userbot panel ─────────────────────────────────────────────────────────
    elif data == "af:userbot":
        await _show_userbot_panel(query, uid)

    elif data == "af:ub_login":
        af_states[uid] = {"step": "waiting_phone", "phone_client": None}
        await query.message.edit_text(
            "<b>👤 Userbot Login — Step 1/3</b>\n\n"
            "Send your Telegram phone number (with country code).\n"
            "<code>Example: +447911123456</code>",
            reply_markup=_cancel_kb(),
        )

    elif data == "af:ub_logout":
        from plugins.af_engine import stop_userbot_af
        await stop_userbot_af(uid)
        await db.remove_userbot(uid)
        await query.answer("✅ Userbot logged out!", show_alert=True)
        await _show_userbot_panel(query, uid)

    await query.answer()


# ── Conversation handler ───────────────────────────────────────────────────────

@Client.on_message(
    filters.private & ~filters.command(BLOCKED_CMDS),
    group=10,
)
async def af_conversation(client, message: Message):
    uid   = message.from_user.id
    state = af_states.get(uid)
    if not state:
        return

    step = state["step"]

    # ── Adding source / target: expect forwarded msg or username text ─────────
    if step in ("waiting_source", "waiting_target"):
        chat_id, chat_title = await _resolve_channel(client, message)
        if chat_id is None:
            await message.reply_text(
                "❌ Couldn't identify a channel.\n\n"
                "Please <b>forward any message</b> from the channel,\n"
                "or send its <b>@username</b> / invite link.",
                reply_markup=_cancel_kb(),
            )
            return

        if step == "waiting_source":
            result = await db.add_af_source(uid, chat_id, chat_title)
            af_states.pop(uid, None)
            if result == "exists":
                await message.reply_text(
                    f"⚠️ <b>{chat_title}</b> is already in your sources.",
                    reply_markup=InlineKeyboardMarkup([[
                        InlineKeyboardButton("📥 Sources", callback_data="af:sources"),
                        InlineKeyboardButton("🔙 Menu",    callback_data="af:menu"),
                    ]]),
                )
            else:
                await message.reply_text(
                    f"✅ <b>Source added:</b> {chat_title}\n<code>{chat_id}</code>\n\n"
                    "Files posted here will be forwarded to all your target channels.",
                    reply_markup=InlineKeyboardMarkup([[
                        InlineKeyboardButton("📥 Sources",    callback_data="af:sources"),
                        InlineKeyboardButton("➕ Add Another", callback_data="af:addsrc"),
                    ]]),
                )
        else:  # waiting_target
            result = await db.add_af_target(uid, chat_id, chat_title)
            af_states.pop(uid, None)
            if result == "exists":
                await message.reply_text(
                    f"⚠️ <b>{chat_title}</b> is already in your targets.",
                    reply_markup=InlineKeyboardMarkup([[
                        InlineKeyboardButton("📤 Targets", callback_data="af:targets"),
                        InlineKeyboardButton("🔙 Menu",    callback_data="af:menu"),
                    ]]),
                )
            else:
                await message.reply_text(
                    f"✅ <b>Target added:</b> {chat_title}\n<code>{chat_id}</code>\n\n"
                    "All source files will be copied here.",
                    reply_markup=InlineKeyboardMarkup([[
                        InlineKeyboardButton("📤 Targets",    callback_data="af:targets"),
                        InlineKeyboardButton("➕ Add Another", callback_data="af:addtgt"),
                    ]]),
                )

    # ── Userbot OTP flow ──────────────────────────────────────────────────────

    elif step == "waiting_phone":
        if not message.text:
            return
        phone = message.text.strip()
        wait_msg = await message.reply_text("📡 Sending OTP…")
        try:
            phone_client = Client(
                ":memory:", Config.API_ID, Config.API_HASH
            )
            await phone_client.connect()
            code = await phone_client.send_code(phone)
        except FloodWait as e:
            await wait_msg.edit_text(f"⏳ FloodWait — please wait {e.value}s and try again.")
            await phone_client.disconnect()
            af_states.pop(uid, None)
            return
        except PhoneNumberInvalid:
            await wait_msg.edit_text("❌ Invalid phone number. Please try again.")
            af_states.pop(uid, None)
            return
        except Exception as e:
            await wait_msg.edit_text(f"❌ Error: <code>{e}</code>")
            af_states.pop(uid, None)
            return

        state["phone_client"]    = phone_client
        state["phone_number"]    = phone
        state["phone_code_hash"] = code.phone_code_hash
        state["step"]            = "waiting_otp"

        await wait_msg.edit_text(
            "<b>👤 Userbot Login — Step 2/3</b>\n\n"
            "✅ OTP sent to your Telegram account.\n\n"
            "Send the OTP with <b>spaces between digits</b>:\n"
            "<code>1 2 3 4 5</code>",
            reply_markup=_cancel_kb(),
        )

    elif step == "waiting_otp":
        if not message.text:
            return
        otp = message.text.strip().replace(" ", "")
        phone_client  = state.get("phone_client")
        phone_number  = state.get("phone_number")
        code_hash     = state.get("phone_code_hash")
        if not phone_client:
            af_states.pop(uid, None)
            return

        try:
            await phone_client.sign_in(phone_number, code_hash, otp)
        except PhoneCodeInvalid:
            await message.reply_text(
                "❌ Invalid OTP. Send the correct code.",
                reply_markup=_cancel_kb(),
            )
            return
        except PhoneCodeExpired:
            await message.reply_text(
                "❌ OTP expired. Please restart login.",
                reply_markup=_back_kb("af:ub_login"),
            )
            af_states.pop(uid, None)
            return
        except SessionPasswordNeeded:
            state["step"] = "waiting_2fa"
            await message.reply_text(
                "<b>👤 Userbot Login — Step 3/3</b>\n\n"
                "🔐 Your account has Two-Step Verification enabled.\n\n"
                "Send your <b>2FA password</b>:",
                reply_markup=_cancel_kb(),
            )
            return
        except Exception as e:
            await message.reply_text(f"❌ Error: <code>{e}</code>")
            af_states.pop(uid, None)
            return

        await _finish_userbot_login(client, message, uid, phone_client)

    elif step == "waiting_2fa":
        if not message.text:
            return
        password     = message.text.strip()
        phone_client = state.get("phone_client")
        if not phone_client:
            af_states.pop(uid, None)
            return

        try:
            await phone_client.check_password(password)
        except PasswordHashInvalid:
            await message.reply_text(
                "❌ Wrong password. Try again.",
                reply_markup=_cancel_kb(),
            )
            return
        except Exception as e:
            await message.reply_text(f"❌ Error: <code>{e}</code>")
            af_states.pop(uid, None)
            return

        await _finish_userbot_login(client, message, uid, phone_client)


# ── Helpers ───────────────────────────────────────────────────────────────────

async def _finish_userbot_login(bot_client, message, uid, phone_client):
    """Export session, store in DB, start AF listener."""
    try:
        session_string = await phone_client.export_session_string()
        me = await phone_client.get_me()
        await phone_client.disconnect()
    except Exception as e:
        await message.reply_text(f"❌ Could not export session: <code>{e}</code>")
        af_states.pop(uid, None)
        return

    details = {
        "id":       me.id,
        "is_bot":   False,
        "user_id":  uid,
        "name":     me.first_name,
        "session":  session_string,
        "username": me.username or "",
    }
    await db.add_userbot(details)
    af_states.pop(uid, None)

    # Start userbot AF listener immediately
    from plugins.af_engine import start_userbot_af
    try:
        await start_userbot_af(uid, session_string)
        ub_note = "✅ Userbot AF listener is now active."
    except Exception as e:
        logger.error(f"[autoforward] Failed to start userbot AF for {uid}: {e}")
        ub_note = "⚠️ Could not start AF listener automatically — it will start on next bot restart."

    await message.reply_text(
        f"🎉 <b>Userbot logged in!</b>\n\n"
        f"<b>Name:</b> {me.first_name}\n"
        f"<b>Username:</b> @{me.username or 'N/A'}\n\n"
        f"{ub_note}",
        reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton("🔙 Back to Menu", callback_data="af:menu")
        ]]),
    )


async def _show_userbot_panel(query: CallbackQuery, uid: int):
    from plugins.af_engine import _running_userbots
    ub = await db.get_userbot(uid)

    if ub:
        running = uid in _running_userbots
        status  = "🟢 Running" if running else "🔴 Stopped (will resume on restart)"
        name    = ub.get("name", "N/A")
        uname   = ub.get("username", "")
        text    = (
            f"<b>👤 Userbot</b>\n\n"
            f"<b>Account:</b> {name}" + (f" (@{uname})" if uname else "") + "\n"
            f"<b>Status:</b>  {status}\n\n"
            "The userbot monitors <b>private source channels</b> and forwards\n"
            "files to your target channels."
        )
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("🔴 Logout Userbot", callback_data="af:ub_logout")],
            [InlineKeyboardButton("🔙 Back",           callback_data="af:menu")],
        ])
    else:
        text = (
            "<b>👤 Userbot</b>\n\n"
            "No userbot configured.\n\n"
            "A userbot lets the bot access <b>private channels</b> you're a member of.\n\n"
            "<b>What you need:</b>\n"
            "  • Your Telegram account (phone number)\n"
            "  • The OTP Telegram sends you\n"
            "  • Your 2FA password (if enabled)\n\n"
            "Your session is stored securely in MongoDB — "
            "only you can log it out."
        )
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("🔑 Login Userbot", callback_data="af:ub_login")],
            [InlineKeyboardButton("🔙 Back",          callback_data="af:menu")],
        ])

    await query.message.edit_text(text, reply_markup=kb)


async def _cancel_temp_client(uid: int):
    """Disconnect any in-progress phone_client during OTP flow."""
    state = af_states.get(uid, {})
    pc    = state.get("phone_client")
    if pc:
        try:
            await pc.disconnect()
        except Exception:
            pass


async def _resolve_channel(client, message: Message):
    """Return (chat_id, chat_title) from a forwarded msg or @username/link text."""
    if message.forward_from_chat:
        chat = message.forward_from_chat
        if chat.type in [enums.ChatType.CHANNEL, enums.ChatType.SUPERGROUP]:
            return chat.id, chat.title or str(chat.id)

    if message.text:
        text = message.text.strip()
        try:
            chat = await client.get_chat(text)
            return chat.id, chat.title or str(chat.id)
        except Exception:
            pass

    return None, None
