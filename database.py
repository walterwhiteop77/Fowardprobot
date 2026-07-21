import motor.motor_asyncio
from config import Config

# ── Auto-forward defaults ─────────────────────────────────────────────────────

AF_DEFAULT_FILTERS: dict = {
    "types":       [
        "video", "document", "photo", "audio", "voice", "animation",
        "video_note", "sticker",
    ],
    "min_size_mb": 0,
    "max_size_mb": 0,
    "keywords":    [],
    "extensions":  [],
}
AF_DEFAULT_SPEED: str = "normal"


class Db:

    def __init__(self, uri, database_name):
        self._client = motor.motor_asyncio.AsyncIOMotorClient(uri)
        self.db = self._client[database_name]
        self.bot     = self.db.bots
        self.userbot = self.db.userbot 
        self.col     = self.db.users
        self.nfy     = self.db.notify
        self.chl     = self.db.channels
        # ── Auto-forward config (flat sources + targets per user) ─────────
        # Schema: { user_id, sources: [{id, title}], targets: [{id, title}] }
        self.afc = self.db.af_config

        # ── Allowlist (access control) ────────────────────────────────────
        # wl      : { user_id: int }  — whitelisted users
        # bot_cfg : { key: str, value: any }  — bot-wide settings (e.g. allow_mode)
        self.wl      = self.db.whitelist
        self.bot_cfg = self.db.bot_config

    # ── User helpers ──────────────────────────────────────────────────────────

    def new_user(self, id, name):
        return dict(
            id=id, name=name,
            ban_status=dict(is_banned=False, ban_reason=""),
        )

    async def add_user(self, id, name):
        await self.col.insert_one(self.new_user(id, name))

    async def is_user_exist(self, id):
        return bool(await self.col.find_one({'id': int(id)}))

    async def total_users_count(self):
        return await self.col.count_documents({})

    async def total_users_bots_count(self):
        bcount = await self.bot.count_documents({})
        count  = await self.col.count_documents({})
        return count, bcount

    async def remove_ban(self, id):
        await self.col.update_one({'id': id}, {'$set': {'ban_status': dict(is_banned=False, ban_reason='')}})

    async def ban_user(self, user_id, ban_reason="No Reason"):
        await self.col.update_one({'id': user_id}, {'$set': {'ban_status': dict(is_banned=True, ban_reason=ban_reason)}})

    async def get_ban_status(self, id):
        default = dict(is_banned=False, ban_reason='')
        user = await self.col.find_one({'id': int(id)})
        return user.get('ban_status', default) if user else default

    async def get_all_users(self):
        return self.col.find({})

    async def delete_user(self, user_id):
        await self.col.delete_many({'id': int(user_id)})

    async def get_banned(self):
        users = self.col.find({'ban_status.is_banned': True})
        return [u['id'] async for u in users]

    async def update_configs(self, id, configs):
        await self.col.update_one({'id': int(id)}, {'$set': {'configs': configs}})

    async def get_configs(self, id):
        default = {
            'caption': None, 'duplicate': True, 'forward_tag': False,
            'min_size': 0, 'max_size': 0, 'extension': None,
            'keywords': None, 'protect': None, 'button': None, 'db_uri': None,
            'filters': {
                'poll': True, 'text': True, 'audio': True, 'voice': True,
                'video': True, 'photo': True, 'document': True,
                'animation': True, 'sticker': True,
            }
        }
        user = await self.col.find_one({'id': int(id)})
        return user.get('configs', default) if user else default

    # ── Bot / Userbot helpers ─────────────────────────────────────────────────

    async def add_bot(self, datas):
        if not await self.is_bot_exist(datas['user_id']):
            await self.bot.insert_one(datas)

    async def remove_bot(self, user_id):
        await self.bot.delete_many({'user_id': int(user_id)})

    async def get_bot(self, user_id):
        return await self.bot.find_one({'user_id': user_id})

    async def is_bot_exist(self, user_id):
        return bool(await self.bot.find_one({'user_id': user_id}))

    async def add_userbot(self, datas):
        if await self.is_userbot_exist(datas['user_id']):
            await self.userbot.update_one(
                {'user_id': datas['user_id']}, {'$set': datas}
            )
        else:
            await self.userbot.insert_one(datas)

    async def remove_userbot(self, user_id):
        await self.userbot.delete_many({'user_id': int(user_id)})

    async def get_userbot(self, user_id):
        return await self.userbot.find_one({'user_id': user_id})

    async def is_userbot_exist(self, user_id):
        return bool(await self.userbot.find_one({'user_id': user_id}))

    # ── Channel helpers (for manual /forward) ─────────────────────────────────

    async def in_channel(self, user_id, chat_id):
        return bool(await self.chl.find_one({"user_id": int(user_id), "chat_id": int(chat_id)}))

    async def add_channel(self, user_id, chat_id, title, username):
        if await self.in_channel(user_id, chat_id):
            return False
        await self.chl.insert_one({"user_id": user_id, "chat_id": chat_id, "title": title, "username": username})
        return True

    async def remove_channel(self, user_id, chat_id):
        if not await self.in_channel(user_id, chat_id):
            return False
        await self.chl.delete_many({"user_id": int(user_id), "chat_id": int(chat_id)})
        return True

    async def get_channel_details(self, user_id, chat_id):
        return await self.chl.find_one({"user_id": int(user_id), "chat_id": int(chat_id)})

    async def get_user_channels(self, user_id):
        return [c async for c in self.chl.find({"user_id": int(user_id)})]

    async def get_filters(self, user_id):
        f = (await self.get_configs(user_id))['filters']
        return [k for k, v in f.items() if v is False]

    # ── Forward-task helpers (manual bulk forward) ────────────────────────────

    async def add_frwd(self, user_id):
        return await self.nfy.insert_one({'user_id': int(user_id)})

    async def rmve_frwd(self, user_id=0, all=False):
        return await self.nfy.delete_many({} if all else {'user_id': int(user_id)})

    async def get_all_frwd(self):
        return self.nfy.find({})

    async def forwad_count(self):
        return await self.nfy.count_documents({})

    async def is_forwad_exit(self, user):
        return bool(await self.nfy.find_one({'user_id': user}))

    async def get_forward_details(self, user_id):
        default = {
            'chat_id': None, 'forward_id': None, 'toid': None,
            'last_id': None, 'limit': None, 'msg_id': None,
            'start_time': None, 'fetched': 0, 'offset': 0,
            'deleted': 0, 'total': 0, 'duplicate': 0, 'skip': 0, 'filtered': 0
        }
        user = await self.nfy.find_one({'user_id': int(user_id)})
        return user.get('details', default) if user else default

    async def update_forward(self, user_id, details):
        await self.nfy.update_one({'user_id': user_id}, {'$set': {'details': details}})

    # ── Auto-Forward Config helpers ───────────────────────────────────────────
    #
    # One document per user:
    # {
    #   "user_id": int,
    #   "sources": [{"id": int, "title": str}, ...],  ← channels to watch
    #   "targets": [{"id": int, "title": str}, ...],  ← channels to post into
    # }
    #
    # Forwarding rule: every file from ANY source → ALL targets.

    async def get_af_config(self, user_id: int) -> dict:
        doc = await self.afc.find_one({"user_id": int(user_id)})
        if not doc:
            return {"sources": [], "targets": []}
        return doc

    async def _ensure_af_doc(self, user_id: int):
        """Create an empty AF config doc if one doesn't exist."""
        await self.afc.update_one(
            {"user_id": int(user_id)},
            {"$setOnInsert": {"user_id": int(user_id), "sources": [], "targets": []}},
            upsert=True,
        )

    # ── Sources ──

    async def add_af_source(self, user_id: int, chat_id: int, title: str) -> str:
        """Add a source channel. Returns 'added' or 'exists'."""
        await self._ensure_af_doc(user_id)
        doc = await self.afc.find_one({"user_id": int(user_id)})
        if any(s["id"] == int(chat_id) for s in doc.get("sources", [])):
            return "exists"
        await self.afc.update_one(
            {"user_id": int(user_id)},
            {"$push": {"sources": {"id": int(chat_id), "title": title}}},
        )
        return "added"

    async def touch_af_config(self, user_id: int):
        """Ensure the AF config exists and bump it so listeners can refresh."""
        await self._ensure_af_doc(user_id)
        await self.afc.update_one(
            {"user_id": int(user_id)},
            {"$set": {"updated_at": __import__("datetime").datetime.utcnow()}},
        )

    async def remove_af_source(self, user_id: int, chat_id: int):
        await self.afc.update_one(
            {"user_id": int(user_id)},
            {"$pull": {"sources": {"id": int(chat_id)}}},
        )

    # ── Targets ──

    async def add_af_target(self, user_id: int, chat_id: int, title: str) -> str:
        """Add a target channel. Returns 'added' or 'exists'."""
        await self._ensure_af_doc(user_id)
        doc = await self.afc.find_one({"user_id": int(user_id)})
        if any(t["id"] == int(chat_id) for t in doc.get("targets", [])):
            return "exists"
        await self.afc.update_one(
            {"user_id": int(user_id)},
            {"$push": {"targets": {"id": int(chat_id), "title": title}}},
        )
        return "added"

    async def remove_af_target(self, user_id: int, chat_id: int):
        await self.afc.update_one(
            {"user_id": int(user_id)},
            {"$pull": {"targets": {"id": int(chat_id)}}},
        )

    # ── Engine lookups ──

    async def get_all_af_configs(self) -> list:
        """Return all AF config documents across all users."""
        return [doc async for doc in self.afc.find({})]

    async def get_source_users(self, source_id: int) -> list:
        """
        Return list of (user_id, [target_ids], full_cfg_doc) for every user
        who has source_id in their sources list.
        Used by the bot-side channel handler so filters/speed are accessible.
        """
        results = []
        async for doc in self.afc.find({"sources.id": int(source_id)}):
            targets = [t["id"] for t in doc.get("targets", [])]
            if targets:
                results.append((doc["user_id"], targets, doc))
        return results

    # ── Filter / Speed helpers ─────────────────────────────────────────────────

    async def get_af_filters(self, user_id: int) -> dict:
        """Return the user's AF filter config, defaulting to AF_DEFAULT_FILTERS."""
        doc = await self.afc.find_one({"user_id": int(user_id)})
        if doc and "filters" in doc:
            return doc["filters"]
        return dict(AF_DEFAULT_FILTERS)

    async def set_af_filters(self, user_id: int, filters_cfg: dict):
        """Persist the user's AF filter config."""
        await self.afc.update_one(
            {"user_id": int(user_id)},
            {"$set": {"filters": filters_cfg}},
            upsert=True,
        )

    async def get_af_speed(self, user_id: int) -> str:
        """Return the user's AF speed mode, defaulting to AF_DEFAULT_SPEED."""
        doc = await self.afc.find_one({"user_id": int(user_id)})
        if doc:
            return doc.get("speed", AF_DEFAULT_SPEED)
        return AF_DEFAULT_SPEED

    async def set_af_speed(self, user_id: int, speed: str):
        """Persist the user's AF speed mode ('safe', 'normal', or 'fast')."""
        await self.afc.update_one(
            {"user_id": int(user_id)},
            {"$set": {"speed": speed}},
            upsert=True,
        )


    # ── Allowlist helpers ─────────────────────────────────────────────────────

    async def add_to_whitelist(self, user_id: int) -> bool:
        """Add user to whitelist. Returns False if already present."""
        if await self.is_whitelisted(user_id):
            return False
        await self.wl.insert_one({"user_id": int(user_id)})
        return True

    async def remove_from_whitelist(self, user_id: int) -> bool:
        """Remove user from whitelist. Returns False if not present."""
        result = await self.wl.delete_one({"user_id": int(user_id)})
        return result.deleted_count > 0

    async def is_whitelisted(self, user_id: int) -> bool:
        return bool(await self.wl.find_one({"user_id": int(user_id)}))

    async def get_whitelist(self) -> list:
        return [doc["user_id"] async for doc in self.wl.find({})]

    async def set_allow_mode(self, enabled: bool):
        await self.bot_cfg.update_one(
            {"key": "allow_mode"},
            {"$set": {"value": bool(enabled)}},
            upsert=True,
        )

    async def get_allow_mode(self) -> bool:
        doc = await self.bot_cfg.find_one({"key": "allow_mode"})
        return bool(doc["value"]) if doc else False


db = Db(Config.DATABASE_URI, Config.DATABASE_NAME)
