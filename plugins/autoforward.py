import asyncio
import logging
from database import db
from config import Config, temp
from pyrogram import Client, filters, enums
from pyrogram.types import (
    InlineKeyboardButton, InlineKeyboardMarkup, Message, CallbackQuery
)

logger = logging.getLogger(__name__)

# ─── In-memory conversation state ───────────────────────────────────────────
# { user_id: { "step": str, "source_id": int, "source_title": str } }
af_states = {}

BLOCKED_CMDS = [
    "start", "forward", "settings", "stop", "reset", "restart",
    "resetall", "broadcast", "unequify", "autoforward"
]

# ─── Keyboards ──────────────────────────────────────────────────────────────

def main_menu_kb():
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("📋 My Mappings", callback_data="af:list"),
            InlineKeyboardButton("➕ Add Mapping",  callback_data="af:add"),
        ],
        [InlineKeyboardButton("ℹ️ How It Works", callback_data="af:help")],
    ])


def mappings_kb(mappings):
    buttons = []
    for m in mappings:
        title = m.get("source_title", str(m["source_id"]))[:30]
        count = len(m.get("target_ids", []))
        buttons.append([
            InlineKeyboardButton(
                f"📥 {title}  →  {count} target(s)",
                callback_data=f"af:view:{m['source_id']}"
            )
        ])
    buttons.append([
        InlineKeyboardButton("➕ Add Mapping", callback_data="af:add"),
        InlineKeyboardButton("🔙 Back",        callback_data="af:menu"),
    ])
    return InlineKeyboardMarkup(buttons)


def source_detail_kb(source_id, targets):
    buttons = []
    for t in targets:
        t_id    = t["id"]
        t_title = t.get("title", str(t_id))[:25]
        buttons.append([
            InlineKeyboardButton(f"📤 {t_title}", callback_data="af:noop"),
            InlineKeyboardButton("❌ Remove",
                                 callback_data=f"af:dtgt:{source_id}:{t_id}"),
        ])
    buttons.append([
        InlineKeyboardButton("➕ Add Target",    callback_data=f"af:atgt:{source_id}"),
        InlineKeyboardButton("🗑 Remove Source", callback_data=f"af:dsrc:{source_id}"),
    ])
    buttons.append([InlineKeyboardButton("🔙 Back to List", callback_data="af:list")])
    return InlineKeyboardMarkup(buttons)


def cancel_kb():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("❌ Cancel", callback_data="af:cancel")]
    ])


def after_mapping_kb():
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("📋 My Mappings", callback_data="af:list"),
            InlineKeyboardButton("➕ Add Another", callback_data="af:add"),
        ]
    ])


# ─── /autoforward command ────────────────────────────────────────────────────

@Client.on_message(filters.private & filters.command("autoforward"))
async def autoforward_cmd(client, message: Message):
    user = message.from_user
    if not await db.is_user_exist(user.id):
        await db.add_user(user.id, user.first_name)
    af_states.pop(user.id, None)
    await message.reply_text(
        "<b>🔄 Auto Forward Manager</b>\n\n"
        "Automatically forward files from source channels to target channels "
        "the moment they are posted — no manual action needed.\n\n"
        "<i>Bot must be admin in both source and target channels.</i>",
        reply_markup=main_menu_kb(),
    )


# ─── Callback query handler ──────────────────────────────────────────────────

@Client.on_callback_query(filters.regex(r"^af:"))
async def af_callback(client, query: CallbackQuery):
    user_id = query.from_user.id
    data    = query.data

    # ── Main menu ──
    if data == "af:menu":
        af_states.pop(user_id, None)
        await query.message.edit_text(
            "<b>🔄 Auto Forward Manager</b>\n\n"
            "Automatically forward files from source channels to target channels "
            "the moment they are posted.\n\n"
            "<i>Bot must be admin in both source and target channels.</i>",
            reply_markup=main_menu_kb(),
        )

    # ── List all mappings ──
    elif data == "af:list":
        af_states.pop(user_id, None)
        mappings = await db.get_af_mappings(user_id)
        if not mappings:
            await query.message.edit_text(
                "<b>📋 My Mappings</b>\n\n"
                "You have no auto-forward mappings yet.\n\n"
                "Tap <b>➕ Add Mapping</b> to link a source channel to a target channel.",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("➕ Add Mapping", callback_data="af:add")],
                    [InlineKeyboardButton("🔙 Back",        callback_data="af:menu")],
                ]),
            )
        else:
            await query.message.edit_text(
                f"<b>📋 My Mappings</b>  ({len(mappings)} source(s))\n\n"
                "Tap a source to manage its targets:",
                reply_markup=mappings_kb(mappings),
            )

    # ── Start add-mapping flow ──
    elif data == "af:add":
        af_states[user_id] = {"step": "waiting_source"}
        await query.message.edit_text(
            "<b>➕ Add Mapping — Step 1 of 2</b>\n\n"
            "📥 <b>Set Source Channel</b>\n\n"
            "Forward any message from the <b>source channel</b> into this chat.\n"
            "<i>(The bot must already be an admin in that channel.)</i>",
            reply_markup=cancel_kb(),
        )

    # ── Cancel ──
    elif data == "af:cancel":
        af_states.pop(user_id, None)
        await query.message.edit_text(
            "❌ <b>Cancelled.</b>",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 Back to Menu", callback_data="af:menu")]
            ]),
        )

    # ── Help ──
    elif data == "af:help":
        await query.message.edit_text(
            "<b>ℹ️ How Auto Forward Works</b>\n\n"
            "1️⃣  Tap <b>Add Mapping</b>\n"
            "2️⃣  Forward any message from your <b>source channel</b>\n"
            "3️⃣  Forward any message from your <b>target channel</b>\n"
            "4️⃣  Done! Forwarding starts immediately 🚀\n\n"
            "<b>Tips:</b>\n"
            "• You can add <b>multiple targets</b> per source\n"
            "• You can add <b>multiple sources</b>\n"
            "• Forwards videos, documents, photos and audio\n"
            "• Files appear <b>without</b> the 'Forwarded from' tag\n"
            "• Bot needs <b>admin</b> rights in every channel",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 Back", callback_data="af:menu")]
            ]),
        )

    # ── No-op (label buttons) ──
    elif data == "af:noop":
        await query.answer()
        return

    # ── View source detail ──
    elif data.startswith("af:view:"):
        source_id = int(data.split(":")[2])
        mapping = await db.get_af_mapping_by_source(user_id, source_id)
        if not mapping:
            await query.answer("Mapping not found!", show_alert=True)
            return
        source_title = mapping.get("source_title", str(source_id))
        targets      = mapping.get("target_ids", [])
        target_lines = "\n".join(
            f"  • {t.get('title', str(t['id']))}" for t in targets
        ) or "  (none yet)"
        await query.message.edit_text(
            f"<b>📥 Source: {source_title}</b>\n"
            f"<code>{source_id}</code>\n\n"
            f"<b>📤 Targets ({len(targets)}):</b>\n{target_lines}",
            reply_markup=source_detail_kb(source_id, targets),
        )

    # ── Remove entire source ──
    elif data.startswith("af:dsrc:"):
        source_id = int(data.split(":")[2])
        await db.remove_af_source(user_id, source_id)
        await query.answer("✅ Source removed!", show_alert=True)
        mappings = await db.get_af_mappings(user_id)
        if not mappings:
            await query.message.edit_text(
                "<b>📋 My Mappings</b>\n\nNo mappings left.",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("➕ Add Mapping", callback_data="af:add")],
                    [InlineKeyboardButton("🔙 Back",        callback_data="af:menu")],
                ]),
            )
        else:
            await query.message.edit_text(
                f"<b>📋 My Mappings</b>  ({len(mappings)} source(s))",
                reply_markup=mappings_kb(mappings),
            )

    # ── Remove one target ──
    elif data.startswith("af:dtgt:"):
        parts     = data.split(":")
        source_id = int(parts[2])
        target_id = int(parts[3])
        await db.remove_af_target(user_id, source_id, target_id)
        await query.answer("✅ Target removed!", show_alert=True)
        mapping = await db.get_af_mapping_by_source(user_id, source_id)
        if not mapping or not mapping.get("target_ids"):
            await db.remove_af_source(user_id, source_id)
            mappings = await db.get_af_mappings(user_id)
            text = f"<b>📋 My Mappings</b>  ({len(mappings)} source(s))" if mappings else \
                   "<b>📋 My Mappings</b>\n\nNo mappings left."
            kb = mappings_kb(mappings) if mappings else InlineKeyboardMarkup([
                [InlineKeyboardButton("➕ Add Mapping", callback_data="af:add")],
                [InlineKeyboardButton("🔙 Back",        callback_data="af:menu")],
            ])
            await query.message.edit_text(text, reply_markup=kb)
        else:
            source_title = mapping.get("source_title", str(source_id))
            targets      = mapping.get("target_ids", [])
            target_lines = "\n".join(
                f"  • {t.get('title', str(t['id']))}" for t in targets
            )
            await query.message.edit_text(
                f"<b>📥 Source: {source_title}</b>\n"
                f"<code>{source_id}</code>\n\n"
                f"<b>📤 Targets ({len(targets)}):</b>\n{target_lines}",
                reply_markup=source_detail_kb(source_id, targets),
            )

    # ── Add another target to existing source ──
    elif data.startswith("af:atgt:"):
        source_id = int(data.split(":")[2])
        mapping = await db.get_af_mapping_by_source(user_id, source_id)
        if not mapping:
            await query.answer("Source not found!", show_alert=True)
            return
        source_title = mapping.get("source_title", str(source_id))
        af_states[user_id] = {
            "step":         "waiting_target",
            "source_id":    source_id,
            "source_title": source_title,
        }
        await query.message.edit_text(
            f"<b>➕ Add Target to {source_title}</b>\n\n"
            "📤 Forward any message from the <b>target channel</b> here.\n"
            "<i>(The bot must already be an admin in that channel.)</i>",
            reply_markup=cancel_kb(),
        )

    await query.answer()


# ─── Conversation handler (private messages during setup flow) ───────────────

@Client.on_message(
    filters.private
    & ~filters.command(BLOCKED_CMDS)
    & (filters.forwarded | filters.text),
    group=10
)
async def af_conversation(client, message: Message):
    user_id = message.from_user.id
    state   = af_states.get(user_id)
    if not state:
        return

    step = state["step"]

    # ── Step 1: capture source channel ──
    if step == "waiting_source":
        chat_id, chat_title = await resolve_channel(client, message)
        if chat_id is None:
            await message.reply_text(
                "❌ Couldn't identify a channel.\n\n"
                "Please <b>forward any message</b> from the source channel,\n"
                "or send its <b>@username</b> or <b>invite link</b>.",
                reply_markup=cancel_kb(),
            )
            return

        af_states[user_id] = {
            "step":         "waiting_target",
            "source_id":    chat_id,
            "source_title": chat_title,
        }
        await message.reply_text(
            f"✅ Source saved: <b>{chat_title}</b>\n"
            f"<code>{chat_id}</code>\n\n"
            "<b>➕ Add Mapping — Step 2 of 2</b>\n\n"
            "📤 Now forward any message from the <b>target channel</b> here.\n"
            "<i>(The bot must already be an admin in that channel.)</i>",
            reply_markup=cancel_kb(),
        )

    # ── Step 2: capture target channel ──
    elif step == "waiting_target":
        chat_id, chat_title = await resolve_channel(client, message)
        if chat_id is None:
            await message.reply_text(
                "❌ Couldn't identify a channel.\n\n"
                "Please <b>forward any message</b> from the target channel,\n"
                "or send its <b>@username</b> or <b>invite link</b>.",
                reply_markup=cancel_kb(),
            )
            return

        source_id    = state["source_id"]
        source_title = state["source_title"]

        if chat_id == source_id:
            await message.reply_text(
                "❌ Source and target cannot be the same channel!",
                reply_markup=cancel_kb(),
            )
            return

        result = await db.add_af_target(
            user_id, source_id, source_title, chat_id, chat_title
        )
        af_states.pop(user_id, None)

        if result == "exists":
            await message.reply_text(
                f"⚠️ <b>{chat_title}</b> is already a target for <b>{source_title}</b>.",
                reply_markup=after_mapping_kb(),
            )
        else:
            await message.reply_text(
                f"🎉 <b>Mapping Created!</b>\n\n"
                f"📥 <b>Source:</b>  {source_title}\n"
                f"   <code>{source_id}</code>\n\n"
                f"📤 <b>Target:</b>  {chat_title}\n"
                f"   <code>{chat_id}</code>\n\n"
                f"Files posted in <b>{source_title}</b> will now be auto-forwarded "
                f"to <b>{chat_title}</b> instantly! 🚀",
                reply_markup=after_mapping_kb(),
            )


# ─── Helper: extract channel id+title from any message ──────────────────────

async def resolve_channel(client, message: Message):
    """Return (chat_id, chat_title) from a forwarded msg or username/link text."""
    # Forwarded from a channel
    if message.forward_from_chat:
        chat = message.forward_from_chat
        if chat.type in [enums.ChatType.CHANNEL, enums.ChatType.SUPERGROUP]:
            return chat.id, chat.title or str(chat.id)

    # Plain text: username, @username, or t.me link
    if message.text:
        text = message.text.strip()
        try:
            chat = await client.get_chat(text)
            return chat.id, chat.title or str(chat.id)
        except Exception:
            pass

    return None, None
