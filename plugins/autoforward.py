"""
Auto Forward Plugin — Interactive management UI
────────────────────────────────────────────────
/autoforward  →  main menu

Panels
  📥 Sources  — channels to watch
  📤 Targets  — channels to post into
  👤 Userbot  — optional user-session for private-channel access
  📊 Status   — live summary
  🎛 Filters  — per-media-type toggle, size limits, keywords, extensions
  ⚡ Speed    — 🐢 Safe / ⚡ Normal / 🚀 Fast
  ℹ️ Help

Forwarding rule: every file from ANY source → ALL targets (many-to-many).
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
from database import db, AF_DEFAULT_FILTERS, AF_DEFAULT_SPEED

logger = logging.getLogger(__name__)

# ── Conversation state ────────────────────────────────────────────────────────
af_states: dict = {}

BLOCKED_CMDS = [
    "start", "forward", "settings", "stop", "reset", "restart",
    "resetall", "broadcast", "unequify", "autoforward",
]

# ── Speed metadata (mirrors af_engine constants) ──────────────────────────────
SPEED_META = {
    "safe":   {
        "label": "🐢 Safe",
        "desc":  "Max protection · ~1 msg / 8 s extra delay · lowest ban risk",
    },
    "normal": {
        "label": "⚡ Normal",
        "desc":  "Balanced · ~1 msg / 5 s extra delay · recommended",
    },
    "fast":   {
        "label": "🚀 Fast",
        "desc":  "Fastest · no extra delay · rate-limiter only · use with care",
    },
}

TYPE_META = [
    ("video",     "🎬 Video"),
    ("document",  "📄 Docs"),
    ("photo",     "🖼 Photo"),
    ("audio",     "🎵 Audio"),
    ("voice",     "🎤 Voice"),
    ("animation", "🎭 Anim"),
    ("video_note", "📹 VNote"),
    ("sticker",   "🏷 Sticker"),
]


# ── Keyboards ─────────────────────────────────────────────────────────────────

def _main_kb():
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("📥 Sources", callback_data="af:sources"),
            InlineKeyboardButton("📤 Targets", callback_data="af:targets"),
        ],
        [
            InlineKeyboardButton("👤 Userbot", callback_data="af:userbot"),
            InlineKeyboardButton("📊 Status",  callback_data="af:status"),
        ],
        [
            InlineKeyboardButton("🎛 Filters", callback_data="af:filters"),
            InlineKeyboardButton("⚡ Speed",   callback_data="af:speed"),
        ],
        [InlineKeyboardButton("ℹ️ How It Works", callback_data="af:help")],
    ])


def _list_kb(items, remove_prefix, add_cb, back_cb="af:menu"):
    rows = []
    for it in items:
        label = it.get("title", str(it["id"]))[:28]
        rows.append([
            InlineKeyboardButton(f"• {label}", callback_data="af:noop"),
            InlineKeyboardButton("❌", callback_data=f"{remove_prefix}:{it['id']}"),
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


def _filters_kb(f: dict):
    enabled = f.get("types", AF_DEFAULT_FILTERS["types"])

    def _type_btn(key, label):
        tick = "✅" if key in enabled else "☑️"
        return InlineKeyboardButton(f"{label} {tick}", callback_data=f"af:ftoggle:{key}")

    min_mb = f.get("min_size_mb", 0) or 0
    max_mb = f.get("max_size_mb", 0) or 0
    kws    = f.get("keywords",    [])
    exts   = f.get("extensions",  [])

    row_types_1 = [_type_btn(k, l) for k, l in TYPE_META[:3]]
    row_types_2 = [_type_btn(k, l) for k, l in TYPE_META[3:6]]
    row_types_3 = [_type_btn(k, l) for k, l in TYPE_META[6:]]

    return InlineKeyboardMarkup([
        row_types_1,
        row_types_2,
        row_types_3,
        [
            InlineKeyboardButton(
                f"📏 Min: {min_mb} MB" if min_mb else "📏 Min Size: Off",
                callback_data="af:fminsize",
            ),
            InlineKeyboardButton(
                f"📏 Max: {max_mb} MB" if max_mb else "📏 Max Size: ∞",
                callback_data="af:fmaxsize",
            ),
        ],
        [
            InlineKeyboardButton(
                f"🔤 Keywords ({len(kws)})" if kws else "🔤 Keywords: Any",
                callback_data="af:fkw",
            ),
            InlineKeyboardButton(
                f"📎 Ext ({len(exts)})" if exts else "📎 Extensions: All",
                callback_data="af:fext",
            ),
        ],
        [
            InlineKeyboardButton("🔄 Reset Filters", callback_data="af:freset"),
            InlineKeyboardButton("🔙 Back",          callback_data="af:menu"),
        ],
    ])


def _speed_kb(current: str):
    btns = []
    for key, meta in SPEED_META.items():
        label = meta["label"] + (" ✅" if key == current else "")
        btns.append(InlineKeyboardButton(label, callback_data=f"af:spd:{key}"))
    return InlineKeyboardMarkup([
        btns,
        [InlineKeyboardButton("🔙 Back", callback_data="af:menu")],
    ])


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
        cfg  = await db.get_af_config(uid)
        srcs = cfg.get("sources", [])
        text = (
            f"<b>📥 Source Channels ({len(srcs)})</b>\n\n" +
            "\n".join(f"  • {s.get('title', s['id'])}" for s in srcs) +
            "\n\nFiles from <b>any</b> of these are forwarded to <b>all</b> targets."
        ) if srcs else "<b>📥 Source Channels</b>\n\nNo sources yet. Add one!"
        await query.message.edit_text(
            text,
            reply_markup=_list_kb(srcs, "af:dsrc", "af:addsrc", "af:menu"),
        )

    # ── Targets list ──────────────────────────────────────────────────────────
    elif data == "af:targets":
        af_states.pop(uid, None)
        cfg  = await db.get_af_config(uid)
        tgts = cfg.get("targets", [])
        text = (
            f"<b>📤 Target Channels ({len(tgts)})</b>\n\n" +
            "\n".join(f"  • {t.get('title', t['id'])}" for t in tgts) +
            "\n\nFiles are copied into <b>all</b> of these when any source posts."
        ) if tgts else "<b>📤 Target Channels</b>\n\nNo targets yet. Add one!"
        await query.message.edit_text(
            text,
            reply_markup=_list_kb(tgts, "af:dtgt", "af:addtgt", "af:menu"),
        )

    # ── Remove source ─────────────────────────────────────────────────────────
    elif data.startswith("af:dsrc:"):
        chat_id = int(data.split(":")[2])
        await db.remove_af_source(uid, chat_id)
        await query.answer("✅ Source removed!", show_alert=True)
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

    # ── Add source / target ───────────────────────────────────────────────────
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

    # ── Cancel ────────────────────────────────────────────────────────────────
    elif data == "af:cancel":
        await _cancel_temp_client(uid)
        af_states.pop(uid, None)
        await query.message.edit_text(
            "❌ <b>Cancelled.</b>",
            reply_markup=_back_kb("af:menu"),
        )

    # ── Status ────────────────────────────────────────────────────────────────
    elif data == "af:status":
        cfg   = await db.get_af_config(uid)
        srcs  = cfg.get("sources", [])
        tgts  = cfg.get("targets", [])
        ub    = await db.get_userbot(uid)
        speed = cfg.get("speed", AF_DEFAULT_SPEED)

        from plugins.af_engine import _running_userbots
        ub_status = "🟢 Running" if uid in _running_userbots else (
            "🔴 Not started" if ub else "⚪ Not configured"
        )

        f        = cfg.get("filters", AF_DEFAULT_FILTERS)
        types_on = f.get("types", AF_DEFAULT_FILTERS["types"])
        types_str = ", ".join(t.upper() for t in types_on) or "None"
        min_mb   = f.get("min_size_mb", 0) or 0
        max_mb   = f.get("max_size_mb", 0) or 0
        kws      = f.get("keywords",    [])
        exts     = f.get("extensions",  [])
        size_str = f"{min_mb} MB – " + (f"{max_mb} MB" if max_mb else "∞")

        src_lines = "\n".join(f"  📥 {s.get('title', s['id'])}" for s in srcs) or "  (none)"
        tgt_lines = "\n".join(f"  📤 {t.get('title', t['id'])}" for t in tgts) or "  (none)"

        await query.message.edit_text(
            f"<b>📊 Auto Forward Status</b>\n\n"
            f"<b>Sources ({len(srcs)}):</b>\n{src_lines}\n\n"
            f"<b>Targets ({len(tgts)}):</b>\n{tgt_lines}\n\n"
            f"<b>Userbot:</b> {ub_status}\n"
            f"<b>Speed:</b> {SPEED_META[speed]['label']}\n\n"
            f"<b>Filters:</b>\n"
            f"  Types: {types_str}\n"
            f"  Size: {size_str}\n"
            f"  Keywords: {', '.join(kws) or 'Any'}\n"
            f"  Extensions: {', '.join(exts) or 'All'}",
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
            "<b>4. Set Filters</b> (optional)\n"
            "   Go to 🎛 Filters to restrict by file type, size, keyword or extension.\n\n"
            "<b>5. Set Speed</b> (optional)\n"
            "   Go to ⚡ Speed to choose Safe / Normal / Fast.\n\n"
            "<b>6. Done!</b> Files from any source → all targets automatically.\n\n"
            "<b>Permissions needed:</b>\n"
            "  • Sources (public): bot must be admin\n"
            "  • Sources (private): userbot must be a member\n"
            "  • Targets: bot AND userbot must be admin\n\n"
            "Files appear as native posts — no 'Forwarded from' tag.",
            reply_markup=_back_kb("af:menu"),
        )

    # ── No-op ─────────────────────────────────────────────────────────────────
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

    # ── Filters panel ─────────────────────────────────────────────────────────
    elif data == "af:filters":
        af_states.pop(uid, None)
        f = await db.get_af_filters(uid)
        await query.message.edit_text(
            _filters_text(f),
            reply_markup=_filters_kb(f),
        )

    elif data.startswith("af:ftoggle:"):
        type_key = data.split(":")[2]
        f        = await db.get_af_filters(uid)
        types    = list(f.get("types", AF_DEFAULT_FILTERS["types"]))
        if type_key in types:
            if len(types) == 1:
                await query.answer("⚠️ At least one type must be enabled!", show_alert=True)
                return
            types.remove(type_key)
        else:
            types.append(type_key)
        f["types"] = types
        await db.set_af_filters(uid, f)
        await query.message.edit_text(_filters_text(f), reply_markup=_filters_kb(f))
        await query.answer()
        return

    elif data == "af:fminsize":
        af_states[uid] = {"step": "waiting_min_size"}
        await query.message.edit_text(
            "<b>📏 Minimum File Size</b>\n\n"
            "Send the minimum file size in <b>MB</b>.\n"
            "Files smaller than this will be skipped.\n\n"
            "Send <code>0</code> to disable the minimum limit.",
            reply_markup=_cancel_kb(),
        )

    elif data == "af:fmaxsize":
        af_states[uid] = {"step": "waiting_max_size"}
        await query.message.edit_text(
            "<b>📏 Maximum File Size</b>\n\n"
            "Send the maximum file size in <b>MB</b>.\n"
            "Files larger than this will be skipped.\n\n"
            "Send <code>0</code> to disable the maximum limit.",
            reply_markup=_cancel_kb(),
        )

    elif data == "af:fkw":
        af_states[uid] = {"step": "waiting_keywords"}
        f   = await db.get_af_filters(uid)
        cur = ", ".join(f.get("keywords", [])) or "None"
        await query.message.edit_text(
            f"<b>🔤 Filename Keywords Filter</b>\n\n"
            f"Current: <code>{cur}</code>\n\n"
            "Send a <b>comma-separated list</b> of keywords.\n"
            "Only files whose name contains at least one keyword will be forwarded.\n\n"
            "Example: <code>movie, series, episode</code>\n\n"
            "Send <code>clear</code> to remove all keywords (forward everything).",
            reply_markup=_cancel_kb(),
        )

    elif data == "af:fext":
        af_states[uid] = {"step": "waiting_extensions"}
        f   = await db.get_af_filters(uid)
        cur = ", ".join(f.get("extensions", [])) or "None"
        await query.message.edit_text(
            f"<b>📎 File Extension Filter</b>\n\n"
            f"Current: <code>{cur}</code>\n\n"
            "Send a <b>comma-separated list</b> of extensions to allow.\n"
            "Files with other extensions will be skipped.\n\n"
            "Example: <code>mp4, mkv, avi</code>\n\n"
            "Send <code>clear</code> to allow all extensions.",
            reply_markup=_cancel_kb(),
        )

    elif data == "af:freset":
        await db.set_af_filters(uid, dict(AF_DEFAULT_FILTERS))
        await query.answer("✅ Filters reset to defaults!", show_alert=True)
        f = await db.get_af_filters(uid)
        await query.message.edit_text(_filters_text(f), reply_markup=_filters_kb(f))

    # ── Speed panel ───────────────────────────────────────────────────────────
    elif data == "af:speed":
        af_states.pop(uid, None)
        speed = await db.get_af_speed(uid)
        await query.message.edit_text(
            _speed_text(speed),
            reply_markup=_speed_kb(speed),
        )

    elif data.startswith("af:spd:"):
        new_speed = data.split(":")[2]
        if new_speed not in SPEED_META:
            await query.answer("Unknown speed mode.", show_alert=True)
            return
        await db.set_af_speed(uid, new_speed)
        await query.answer(f"✅ Speed set to {SPEED_META[new_speed]['label']}!", show_alert=True)
        await query.message.edit_text(
            _speed_text(new_speed),
            reply_markup=_speed_kb(new_speed),
        )
        return

    await query.answer()


# ── Conversation handler ──────────────────────────────────────────────────────

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

    # ── Source / Target channel input ─────────────────────────────────────────
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
                    f"✅ <b>Source added:</b> {chat_title}\n<code>{chat_id}</code>",
                    reply_markup=InlineKeyboardMarkup([[
                        InlineKeyboardButton("📥 Sources",    callback_data="af:sources"),
                        InlineKeyboardButton("➕ Add Another", callback_data="af:addsrc"),
                    ]]),
                )
                await _ensure_userbot_running(uid)
        else:
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
                    f"✅ <b>Target added:</b> {chat_title}\n<code>{chat_id}</code>",
                    reply_markup=InlineKeyboardMarkup([[
                        InlineKeyboardButton("📤 Targets",    callback_data="af:targets"),
                        InlineKeyboardButton("➕ Add Another", callback_data="af:addtgt"),
                    ]]),
                )
                await _ensure_userbot_running(uid)

    # ── Filter: min size ──────────────────────────────────────────────────────
    elif step == "waiting_min_size":
        try:
            val = float(message.text.strip())
            if val < 0:
                raise ValueError
        except (ValueError, AttributeError):
            await message.reply_text(
                "❌ Please send a valid number (e.g. <code>50</code> for 50 MB).",
                reply_markup=_cancel_kb(),
            )
            return
        f = await db.get_af_filters(uid)
        f["min_size_mb"] = val
        await db.set_af_filters(uid, f)
        af_states.pop(uid, None)
        label = f"{val} MB" if val > 0 else "Off (no minimum)"
        await message.reply_text(
            f"✅ Minimum file size set to <b>{label}</b>.",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("🎛 Back to Filters", callback_data="af:filters"),
            ]]),
        )

    # ── Filter: max size ──────────────────────────────────────────────────────
    elif step == "waiting_max_size":
        try:
            val = float(message.text.strip())
            if val < 0:
                raise ValueError
        except (ValueError, AttributeError):
            await message.reply_text(
                "❌ Please send a valid number (e.g. <code>2000</code> for 2 GB).",
                reply_markup=_cancel_kb(),
            )
            return
        f = await db.get_af_filters(uid)
        f["max_size_mb"] = val
        await db.set_af_filters(uid, f)
        af_states.pop(uid, None)
        label = f"{val} MB" if val > 0 else "∞ (no maximum)"
        await message.reply_text(
            f"✅ Maximum file size set to <b>{label}</b>.",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("🎛 Back to Filters", callback_data="af:filters"),
            ]]),
        )

    # ── Filter: keywords ──────────────────────────────────────────────────────
    elif step == "waiting_keywords":
        text = (message.text or "").strip()
        f    = await db.get_af_filters(uid)
        if text.lower() == "clear":
            f["keywords"] = []
            msg = "✅ Keywords cleared — all filenames accepted."
        else:
            kws = [k.strip() for k in text.split(",") if k.strip()]
            f["keywords"] = kws
            msg = f"✅ Keywords set: <code>{', '.join(kws)}</code>"
        await db.set_af_filters(uid, f)
        af_states.pop(uid, None)
        await message.reply_text(
            msg,
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("🎛 Back to Filters", callback_data="af:filters"),
            ]]),
        )

    # ── Filter: extensions ────────────────────────────────────────────────────
    elif step == "waiting_extensions":
        text = (message.text or "").strip()
        f    = await db.get_af_filters(uid)
        if text.lower() == "clear":
            f["extensions"] = []
            msg = "✅ Extensions cleared — all file types accepted."
        else:
            exts = [e.strip().lstrip(".").lower() for e in text.split(",") if e.strip()]
            f["extensions"] = exts
            msg = f"✅ Extensions set: <code>{', '.join(exts)}</code>"
        await db.set_af_filters(uid, f)
        af_states.pop(uid, None)
        await message.reply_text(
            msg,
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("🎛 Back to Filters", callback_data="af:filters"),
            ]]),
        )

    # ── Userbot OTP flow ──────────────────────────────────────────────────────
    elif step == "waiting_phone":
        if not message.text:
            return
        phone    = message.text.strip()
        wait_msg = await message.reply_text("📡 Sending OTP…")
        try:
            phone_client = Client(":memory:", Config.API_ID, Config.API_HASH)
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
        otp          = message.text.strip().replace(" ", "")
        phone_client = state.get("phone_client")
        phone_number = state.get("phone_number")
        code_hash    = state.get("phone_code_hash")
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

def _filters_text(f: dict) -> str:
    enabled  = f.get("types", AF_DEFAULT_FILTERS["types"])
    type_str = " | ".join(
        f"{l} ✅" if k in enabled else f"{l} ☑️"
        for k, l in TYPE_META
    )
    min_mb   = f.get("min_size_mb", 0) or 0
    max_mb   = f.get("max_size_mb", 0) or 0
    kws      = f.get("keywords",    [])
    exts     = f.get("extensions",  [])
    size_str = f"{min_mb} MB" if min_mb else "Off"
    max_str  = f"{max_mb} MB" if max_mb else "∞"
    return (
        "<b>🎛 Auto Forward Filters</b>\n\n"
        f"<b>File Types:</b>\n{type_str}\n\n"
        f"<b>Size:</b>  min {size_str}  ·  max {max_str}\n"
        f"<b>Keywords:</b>  {', '.join(kws) or 'Any filename'}\n"
        f"<b>Extensions:</b>  {', '.join(exts) or 'All extensions'}\n\n"
        "<i>Tap a button below to change a setting.</i>"
    )


def _speed_text(speed: str) -> str:
    lines = []
    for key, meta in SPEED_META.items():
        mark = " ←" if key == speed else ""
        lines.append(f"  {meta['label']}{mark}  —  {meta['desc']}")
    return (
        "<b>⚡ Forwarding Speed</b>\n\n"
        + "\n".join(lines) + "\n\n"
        "<i>The rate-limiter always enforces Telegram's hard limits regardless "
        "of this setting.  'Fast' adds no extra delay on top of those limits.</i>"
    )


async def _finish_userbot_login(bot_client, message, uid, phone_client):
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


async def _ensure_userbot_running(uid: int):
    """Start saved userbot listener after sources/targets are added."""
    from plugins.af_engine import _running_userbots, start_userbot_af

    if uid in _running_userbots:
        return
    cfg = await db.get_af_config(uid)
    if not cfg.get("sources") or not cfg.get("targets"):
        return
    ub = await db.get_userbot(uid)
    if not ub or not ub.get("session"):
        return
    try:
        await start_userbot_af(uid, ub["session"])
    except Exception as e:
        logger.error(f"[autoforward] Failed to start userbot AF for {uid}: {e}")


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
            "The userbot monitors <b>private source channels</b> and forwards "
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
            "  • Your Telegram phone number\n"
            "  • The OTP Telegram sends you\n"
            "  • Your 2FA password (if enabled)\n\n"
            "Your session is stored securely in MongoDB."
        )
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("🔑 Login Userbot", callback_data="af:ub_login")],
            [InlineKeyboardButton("🔙 Back",          callback_data="af:menu")],
        ])

    await query.message.edit_text(text, reply_markup=kb)


async def _cancel_temp_client(uid: int):
    state = af_states.get(uid, {})
    pc    = state.get("phone_client")
    if pc:
        try:
            await pc.disconnect()
        except Exception:
            pass


async def _resolve_channel(client, message: Message):
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
