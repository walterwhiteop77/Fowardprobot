from os import environ

class Config:
    API_ID       = int(environ.get("API_ID", "26442926"))
    API_HASH     = environ.get("API_HASH", "d091234d2c6e123e6d906d3829eb885b")
    BOT_TOKEN    = environ.get("BOT_TOKEN", "") 
    BOT_SESSION  = environ.get("BOT_SESSION", "filmyflixhd")
    DATABASE_URI  = environ.get("DATABASE_URI", "")
    DATABASE_NAME = environ.get("DATABASE_NAME", "Cluster0")
    BOT_OWNER    = int(environ.get("BOT_OWNER", "6725874739"))

    # ── Keep-alive (free hosting) ────────────────────────────────────────────
    # PORT      : the port aiohttp listens on (Render sets this automatically)
    # PING_URL  : your public Render URL, e.g. https://my-bot.onrender.com
    #             Leave empty to disable the self-ping (not recommended on free tier)
    PORT     = int(environ.get("PORT", 8080))
    PING_URL = environ.get("PING_URL", "https://fowardprobot.onrender.com")

class temp(object): 
    lock = {}
    CANCEL = {}
    forwardings = 0
    BANNED_USERS = []
    IS_FRWD_CHAT = []
    ALLOW_MODE = None   # None = not yet loaded; True/False = cached from DB
