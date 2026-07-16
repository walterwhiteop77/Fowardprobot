import asyncio
import logging
from logging.handlers import RotatingFileHandler
from typing import Union, Optional, AsyncGenerator

from pyrogram import Client as SB, idle

from config import Config
from plugins.regix import restart_forwards
from plugins.af_engine import start_af_queue, start_all_userbot_af
from keepalive import start_keepalive

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        RotatingFileHandler("bot.log", maxBytes=5 * 1024 * 1024, backupCount=2),
        logging.StreamHandler(),
    ],
)

if __name__ == "__main__":

    SteveBotz = SB(
        "Steve-Forward-Bot",
        bot_token=Config.BOT_TOKEN,
        api_id=Config.API_ID,
        api_hash=Config.API_HASH,
        sleep_threshold=120,
        plugins=dict(root="plugins"),
    )

    async def iter_messages(
        self,
        chat_id: Union[int, str],
        limit: int,
        offset: int = 0,
    ) -> Optional[AsyncGenerator]:
        current = offset
        while True:
            new_diff = min(200, limit - current)
            if new_diff <= 0:
                return
            messages = await self.get_messages(
                chat_id, list(range(current, current + new_diff + 1))
            )
            for message in messages:
                yield message
                current += 1

    async def main():
        await SteveBotz.start()
        bot_info = await SteveBotz.get_me()

        # 1. Keep-alive HTTP server + self-ping (free hosting)
        await start_keepalive()

        # 2. Auto-forward queue worker
        await start_af_queue(SteveBotz)

        # 3. Start one userbot Pyrogram client for every user who has
        #    both an AF config and a saved userbot session in the DB.
        await start_all_userbot_af()

        # 4. Resume pending manual-forward tasks from before restart
        await restart_forwards(SteveBotz)

        print(f"✅ Bot @{bot_info.username} started.")
        await idle()

    asyncio.get_event_loop().run_until_complete(main())
