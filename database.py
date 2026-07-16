import motor.motor_asyncio
from config import Config

class Db:

    def __init__(self, uri, database_name):
        self._client = motor.motor_asyncio.AsyncIOMotorClient(uri)
        self.db = self._client[database_name]
        self.bot = self.db.bots
        self.userbot = self.db.userbot 
        self.col = self.db.users
        self.nfy = self.db.notify
        self.chl = self.db.channels
        # ── Auto-forward mappings ──────────────────────────────────────────
        self.af  = self.db.af_mappings   # { user_id, source_id, source_title,
                                         #   target_ids: [{id, title}] }

    # ── User helpers ──────────────────────────────────────────────────────────

    def new_user(self, id, name):
        return dict(
            id = id,
            name = name,
            ban_status=dict(
                is_banned=False,
                ban_reason="",
            ),
        )

    async def add_user(self, id, name):
        user = self.new_user(id, name)
        await self.col.insert_one(user)

    async def is_user_exist(self, id):
        user = await self.col.find_one({'id':int(id)})
        return bool(user)

    async def total_users_count(self):
        count = await self.col.count_documents({})
        return count

    async def total_users_bots_count(self):
        bcount = await self.bot.count_documents({})
        count = await self.col.count_documents({})
        return count, bcount

    async def remove_ban(self, id):
        ban_status = dict(
            is_banned=False,
            ban_reason=''
        )
        await self.col.update_one({'id': id}, {'$set': {'ban_status': ban_status}})

    async def ban_user(self, user_id, ban_reason="No Reason"):
        ban_status = dict(
            is_banned=True,
            ban_reason=ban_reason
        )
        await self.col.update_one({'id': user_id}, {'$set': {'ban_status': ban_status}})

    async def get_ban_status(self, id):
        default = dict(
            is_banned=False,
            ban_reason=''
        )
        user = await self.col.find_one({'id':int(id)})
        if not user:
            return default
        return user.get('ban_status', default)

    async def get_all_users(self):
        return self.col.find({})

    async def delete_user(self, user_id):
        await self.col.delete_many({'id': int(user_id)})

    async def get_banned(self):
        users = self.col.find({'ban_status.is_banned': True})
        b_users = [user['id'] async for user in users]
        return b_users

    async def update_configs(self, id, configs):
        await self.col.update_one({'id': int(id)}, {'$set': {'configs': configs}})

    async def get_configs(self, id):
        default = {
            'caption': None,
            'duplicate': True,
            'forward_tag': False,
            'min_size': 0,
            'max_size': 0,
            'extension': None,
            'keywords': None,
            'protect': None,
            'button': None,
            'db_uri': None,
            'filters': {
               'poll': True,
               'text': True,
               'audio': True,
               'voice': True,
               'video': True,
               'photo': True,
               'document': True,
               'animation': True,
               'sticker': True
            }
        }
        user = await self.col.find_one({'id':int(id)})
        if user:
            return user.get('configs', default)
        return default 

    # ── Bot / Userbot helpers ─────────────────────────────────────────────────

    async def add_bot(self, datas):
       if not await self.is_bot_exist(datas['user_id']):
          await self.bot.insert_one(datas)

    async def remove_bot(self, user_id):
       await self.bot.delete_many({'user_id': int(user_id)})

    async def get_bot(self, user_id: int):
       bot = await self.bot.find_one({'user_id': user_id})
       return bot if bot else None

    async def is_bot_exist(self, user_id):
       bot = await self.bot.find_one({'user_id': user_id})
       return bool(bot)
   
    async def add_userbot(self, datas):
       if not await self.is_userbot_exist(datas['user_id']):
          await self.userbot.insert_one(datas)

    async def remove_userbot(self, user_id):
       await self.userbot.delete_many({'user_id': int(user_id)})

    async def get_userbot(self, user_id: int):
       bot = await self.userbot.find_one({'user_id': user_id})
       return bot if bot else None

    async def is_userbot_exist(self, user_id):
       bot = await self.userbot.find_one({'user_id': user_id})
       return bool(bot)

    # ── Channel helpers ───────────────────────────────────────────────────────

    async def in_channel(self, user_id: int, chat_id: int) -> bool:
       channel = await self.chl.find_one({"user_id": int(user_id), "chat_id": int(chat_id)})
       return bool(channel)

    async def add_channel(self, user_id: int, chat_id: int, title, username):
       channel = await self.in_channel(user_id, chat_id)
       if channel:
         return False
       return await self.chl.insert_one({"user_id": user_id, "chat_id": chat_id, "title": title, "username": username})

    async def remove_channel(self, user_id: int, chat_id: int):
       channel = await self.in_channel(user_id, chat_id )
       if not channel:
         return False
       return await self.chl.delete_many({"user_id": int(user_id), "chat_id": int(chat_id)})

    async def get_channel_details(self, user_id: int, chat_id: int):
       return await self.chl.find_one({"user_id": int(user_id), "chat_id": int(chat_id)})

    async def get_user_channels(self, user_id: int):
       channels = self.chl.find({"user_id": int(user_id)})
       return [channel async for channel in channels]

    async def get_filters(self, user_id):
       filters = []
       filter = (await self.get_configs(user_id))['filters']
       for k, v in filter.items():
          if v == False:
            filters.append(str(k))
       return filters

    # ── Forward-task helpers ──────────────────────────────────────────────────

    async def add_frwd(self, user_id):
       return await self.nfy.insert_one({'user_id': int(user_id)})

    async def rmve_frwd(self, user_id=0, all=False):
       data = {} if all else {'user_id': int(user_id)}
       return await self.nfy.delete_many(data)

    async def get_all_frwd(self):
       return self.nfy.find({})
  
    async def forwad_count(self):
        c = await self.nfy.count_documents({})
        return c
        
    async def is_forwad_exit(self, user):
        u = await self.nfy.find_one({'user_id': user})
        return bool(u)
        
    async def get_forward_details(self, user_id):
        defult = {
            'chat_id': None,
            'forward_id': None,
            'toid': None,
            'last_id': None,
            'limit': None,
            'msg_id': None,
            'start_time': None,
            'fetched': 0,
            'offset': 0,
            'deleted': 0,
            'total': 0,
            'duplicate': 0,
            'skip': 0,
            'filtered' :0
        }
        user = await self.nfy.find_one({'user_id': int(user_id)})
        if user:
            return user.get('details', defult)
        return defult
   
    async def update_forward(self, user_id, details):
        await self.nfy.update_one({'user_id': user_id}, {'$set': {'details': details}})

    # ── Auto-Forward Mapping helpers ──────────────────────────────────────────
    #
    # Schema stored in `af_mappings` collection:
    # {
    #   "user_id":      int,           # Telegram user who created this rule
    #   "source_id":    int,           # Source channel chat_id
    #   "source_title": str,
    #   "target_ids":   [              # List of target channels
    #     { "id": int, "title": str },
    #     ...
    #   ]
    # }

    async def get_af_mappings(self, user_id: int):
        """Return all af mappings for a given user."""
        cursor = self.af.find({"user_id": int(user_id)})
        return await cursor.to_list(length=None)

    async def get_af_mapping_by_source(self, user_id: int, source_id: int):
        """Return the mapping doc for a specific user+source pair."""
        return await self.af.find_one({
            "user_id":   int(user_id),
            "source_id": int(source_id),
        })

    async def add_af_target(
        self,
        user_id:      int,
        source_id:    int,
        source_title: str,
        target_id:    int,
        target_title: str,
    ) -> str:
        """
        Link target_id to source_id for user_id.
        Returns "created" | "added" | "exists".
        """
        existing = await self.af.find_one({
            "user_id":   int(user_id),
            "source_id": int(source_id),
        })

        target_entry = {"id": int(target_id), "title": target_title}

        if existing:
            # Check if target already present
            for t in existing.get("target_ids", []):
                if t["id"] == int(target_id):
                    return "exists"
            # Append new target
            await self.af.update_one(
                {"user_id": int(user_id), "source_id": int(source_id)},
                {
                    "$push": {"target_ids": target_entry},
                    "$set":  {"source_title": source_title},
                },
            )
            return "added"
        else:
            await self.af.insert_one({
                "user_id":      int(user_id),
                "source_id":    int(source_id),
                "source_title": source_title,
                "target_ids":   [target_entry],
            })
            return "created"

    async def remove_af_target(self, user_id: int, source_id: int, target_id: int):
        """Remove a single target from a source mapping."""
        await self.af.update_one(
            {"user_id": int(user_id), "source_id": int(source_id)},
            {"$pull": {"target_ids": {"id": int(target_id)}}},
        )

    async def remove_af_source(self, user_id: int, source_id: int):
        """Delete the entire source mapping (all its targets)."""
        await self.af.delete_one({
            "user_id":   int(user_id),
            "source_id": int(source_id),
        })

    async def get_af_all_targets_for_source(self, source_id: int):
        """
        Return all mapping docs that watch source_id — across ALL users.
        Used by the auto-forward engine to decide where to send a new message.
        """
        cursor = self.af.find({"source_id": int(source_id)})
        return await cursor.to_list(length=None)


db = Db(Config.DATABASE_URI, Config.DATABASE_NAME)
