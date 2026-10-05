# Telegram Cloud Storage Bot (owner only)

Forward a file or send a Google Drive / MEGA / direct link -> choose a destination
(TeraBox, Google Drive, MEGA, Telegram). Live progress bar, sequential queue.

- TeraBox: logs in by itself with TERABOX_EMAIL / TERABOX_PASSWORD (aioterabox) and
  re-logs-in automatically when the session expires. No ndus cookie to copy.
- Runs on Render Free (Docker): `python bot.py`, health endpoint `/health`
- Commands: `/start`, `/status`, `/relogin`

See `.env.example` for all environment variables. Never commit a real `.env`.
