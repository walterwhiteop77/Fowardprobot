# SteveBotz Forward Bot

<b>Auto Restart All User Forwarding After Bot Restarted.</b>

![Typing SVG](https://readme-typing-svg.herokuapp.com/?lines=Welcome+To+SteveBotz+Forward+Bot+!)

## Features

- [x] Public Forward (Bot)
- [x] Private Forward (User Bot)
- [x] Custom Caption
- [x] Custom Button
- [x] Skip Duplicate Messages
- [x] Skip Messages Based On Extensions & Keywords & Size
- [x] Filter Type Of Messages
- [x] Auto Restart Pending Task After Bot Restart
- [x] **Auto Forward** — link multiple source channels to multiple target channels; files are forwarded automatically the moment they are posted (no "Forwarded from" tag)

## Commands

```
start      - check I'm alive
forward    - bulk-forward messages from one channel to another
autoforward - manage auto-forward channel mappings (interactive)
unequify   - delete duplicate media messages in chats
settings   - configure your settings
stop       - stop your ongoing tasks
reset      - reset your settings
restart    - restart server (owner only)
resetall   - reset all users settings (owner only)
broadcast  - broadcast a message to all your users (owner only)
```

## Auto Forward — How To Use

1. Add the bot as **admin** in your source channel(s) and target channel(s).
2. Send `/autoforward` to the bot in private chat.
3. Tap **➕ Add Mapping**.
4. **Forward any message** from the source channel into the bot chat.
5. **Forward any message** from the target channel into the bot chat.
6. Done — the bot will now auto-forward videos, documents, photos and audio from source → target instantly.

You can add **multiple targets per source** and **multiple source channels**.

## Variables

* `API_ID`       — API Id from my.telegram.org
* `API_HASH`     — API Hash from my.telegram.org
* `BOT_TOKEN`    — Bot token from @BotFather
* `BOT_OWNER`    — Telegram Account Id of Owner
* `DATABASE_URI` — MongoDB connection URI (e.g. from MongoDB Atlas)
* `DATABASE_NAME` — (optional) MongoDB database name, default `Cluster0`

## Deploy on Render

1. Push this repo to GitHub.
2. Create a new **Background Worker** service on Render.
3. Set **Start Command** to `python3 main.py`.
4. Add all the environment variables above.
5. Deploy.

## Credits

* **[SteveBotz](https://t.me/SteveBotz)**
