"""Universal Telegram Cloud Storage Bot (owner only).

Sources     : Telegram files, Google Drive links, MEGA links, direct http(s) links
Destinations: TeraBox (official OpenAPI), Google Drive, MEGA (rclone), Telegram
"""
import asyncio
import html
import json
import logging
import mimetypes
import os
import re
import shutil
import subprocess
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Optional
from urllib.parse import unquote, urlparse

import aiohttp
from aiohttp import web
from dotenv import load_dotenv
from pyrogram import Client, filters, idle
from pyrogram.enums import ParseMode
from pyrogram.errors import FloodWait, MessageNotModified
from pyrogram.handlers import CallbackQueryHandler, MessageHandler
from pyrogram.types import InlineKeyboardButton as IKB
from pyrogram.types import InlineKeyboardMarkup as IKM
from pyrogram.types import Message

from gdrive import GDrive
from terabox import TeraBox

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("bot")


def env(name: str, default: str = "") -> str:
    return os.getenv(name, default).strip()


API_ID = int(env("API_ID") or 0)
API_HASH = env("API_HASH")
BOT_TOKEN = env("BOT_TOKEN")
OWNER_IDS = {int(x) for x in re.split(r"[,\s]+", env("OWNER_IDS")) if x.lstrip("-").isdigit()}
PORT = int(env("PORT", "10000"))
DOWNLOAD_DIR = Path(env("DOWNLOAD_DIR", "/tmp/downloads"))
MEGA_USER, MEGA_PASS, MEGA_DIR = env("MEGA_USER"), env("MEGA_PASS"), env("MEGA_DIR", "/TelegramBot")

MB = 1024 * 1024
TG_LIMIT = 2000 * MB
VIDEO_EXT = {".mp4", ".mkv", ".mov", ".avi", ".webm", ".m4v"}
URL_RE = re.compile(r"https?://[^\s<>\"']+")
TERA_HOSTS = ("terabox", "1024tera", "teraboxapp", "teraboxshare", "terafileshare", "terasharefile", "teraboxlink")

app: Client = None  # type: ignore
tb: TeraBox = None  # type: ignore
gd: GDrive = None  # type: ignore
QUEUE: asyncio.Queue = None  # type: ignore
PENDING: dict = {}

HELP = (
    "👋 <b>Cloud Storage Bot</b>\n\n"
    "ভিডিও/ফাইল ফরওয়ার্ড করুন, অথবা Google Drive / MEGA / সরাসরি লিংক পাঠান — তারপর নিচের বাটন থেকে গন্তব্য বেছে নিন।\n\n"
    "/status — কিউ ও TeraBox অবস্থা\n"
    "/relogin — TeraBox-এ জোর করে আবার লগইন"
)


# --------------------------------------------------------------------- helpers
def esc(s) -> str:
    return html.escape(str(s))


def human(n: float) -> str:
    n = float(n)
    for u in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024 or u == "TB":
            return f"{int(n)} B" if u == "B" else f"{n:.1f} {u}"
        n /= 1024
    return "0 B"


def bar(pct: float, width: int = 12) -> str:
    filled = int(width * pct / 100)
    return "█" * filled + "░" * (width - filled)


def fmt_time(sec: float) -> str:
    sec = int(max(sec, 0))
    h, r = divmod(sec, 3600)
    m, s = divmod(r, 60)
    return f"{h:d}:{m:02d}:{s:02d}" if h else f"{m:02d}:{s:02d}"


def check_disk(size: int):
    free = shutil.disk_usage(DOWNLOAD_DIR).free
    if size and free < size * 1.05 + 50 * MB:
        raise RuntimeError(f"ডিস্কে জায়গা নেই (খালি {human(free)}, দরকার {human(size)})")


def classify(url: str) -> str:
    host = urlparse(url).netloc.lower()
    if "drive.google.com" in host or "docs.google.com" in host:
        return "gdrive"
    if any(h in host for h in ("mega.nz", "mega.io", "mega.co.nz")):
        return "mega"
    if any(h in host for h in TERA_HOSTS):
        return "terabox"
    return "http"


async def is_owner(_, __, update) -> bool:
    u = update.from_user
    return bool(u and u.id in OWNER_IDS)


owner = filters.create(is_owner)


# -------------------------------------------------------------------- progress
class Tracker:
    """Edits one Telegram message with a live progress bar (throttled)."""

    def __init__(self, msg: Message, title: str):
        self.msg, self.title, self.stage = msg, title, ""
        self._reset()

    def _reset(self):
        self.last_edit = 0.0
        self.last_t = time.time()
        self.last_cur = 0
        self.speed = 0.0

    def set_stage(self, stage: str):
        self.stage = stage
        self._reset()

    async def update(self, cur: int, total: int):
        now = time.time()
        dt = now - self.last_t
        if dt >= 1:
            inst = (cur - self.last_cur) / dt
            self.speed = inst if self.speed == 0 else 0.7 * self.speed + 0.3 * inst
            self.last_t, self.last_cur = now, cur
        finished = bool(total) and cur >= total
        if now - self.last_edit < 4 and not finished:
            return
        self.last_edit = now
        lines = [f"<b>{esc(self.title)}</b>", "", self.stage]
        if total > 0:
            pct = min(cur / total * 100, 100.0)
            lines.append(f"<code>[{bar(pct)}] {pct:5.1f}%</code>")
            lines.append(f"📦 {human(cur)} / {human(total)}")
        else:
            lines.append(f"📦 {human(cur)}")
        speed_line = f"⚡ {human(self.speed)}/s"
        if total > cur and self.speed > 0:
            speed_line += f"  •  ⏱ ETA {fmt_time((total - cur) / self.speed)}"
        lines.append(speed_line)
        try:
            await self.msg.edit_text("\n".join(lines))
        except FloodWait as e:
            self.last_edit = now + int(e.value)
        except MessageNotModified:
            pass
        except Exception:
            pass


# ------------------------------------------------------------------- downloads
async def http_download(url: str, work: Path, tr: Tracker) -> Path:
    timeout = aiohttp.ClientTimeout(total=None, sock_connect=30, sock_read=120)
    async with aiohttp.ClientSession(timeout=timeout) as s:
        async with s.get(url, allow_redirects=True) as r:
            r.raise_for_status()
            total = int(r.headers.get("Content-Length") or 0)
            check_disk(total)
            name = ""
            cd = r.headers.get("Content-Disposition", "")
            m = re.search(r"filename\*?=(?:UTF-8'')?\"?([^\";]+)", cd)
            if m:
                name = unquote(m.group(1))
            name = name or unquote(os.path.basename(urlparse(str(r.url)).path)) or f"file_{uuid.uuid4().hex[:6]}"
            name = re.sub(r"[\\/:*?\"<>|]", "_", name)
            path = work / name
            cur = 0
            with open(path, "wb") as f:
                async for chunk in r.content.iter_chunked(1 << 20):
                    f.write(chunk)
                    cur += len(chunk)
                    await tr.update(cur, total)
    return path


def dir_size(p: Path) -> int:
    return sum(f.stat().st_size for f in p.rglob("*") if f.is_file())


async def mega_download(url: str, work: Path, tr: Tracker) -> Path:
    if not shutil.which("megadl"):
        raise RuntimeError("megadl ইনস্টল করা নেই")
    proc = await asyncio.create_subprocess_exec(
        "megadl", "--path", str(work), url,
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT,
    )
    tail = ""
    while True:
        chunk = await proc.stdout.read(512)
        if not chunk:
            break
        tail = (tail + chunk.decode(errors="ignore"))[-600:]
        pcts = re.findall(r"(\d{1,3}(?:\.\d+)?)%", tail)
        cur = dir_size(work)
        total = int(cur / (float(pcts[-1]) / 100)) if pcts and float(pcts[-1]) > 0.5 else 0
        await tr.update(cur, total)
    if await proc.wait() != 0:
        raise RuntimeError("MEGA ডাউনলোড ব্যর্থ: " + tail.strip().splitlines()[-1][:200] if tail.strip() else "MEGA ডাউনলোড ব্যর্থ")
    files = [p for p in work.iterdir() if p.is_file() and not p.name.startswith(".")]
    if len(files) != 1:
        raise RuntimeError("MEGA ফোল্ডার লিংক সমর্থিত নয় — শুধু একক ফাইল")
    return files[0]


async def fetch_source(job: "Job", work: Path, tr: Tracker) -> Path:
    tr.set_stage("📥 ডাউনলোড হচ্ছে…")
    if job.kind == "tg":
        check_disk(job.size)
        msg = await app.get_messages(job.chat_id, job.msg_id)
        p = await app.download_media(msg, file_name=f"{work}/", progress=tr.update)
        if not p:
            raise RuntimeError("টেলিগ্রাম থেকে ডাউনলোড হয়নি")
        return Path(p)
    if job.kind == "gdrive":
        check_disk(job.size)
        return await gd.download(job.extra["file_id"], work, tr.update)
    if job.kind == "mega":
        return await mega_download(job.url, work, tr)
    return await http_download(job.url, work, tr)


# ---------------------------------------------------------------- destinations
def rclone_env() -> dict:
    obscured = subprocess.run(["rclone", "obscure", MEGA_PASS], capture_output=True, text=True).stdout.strip()
    return {
        **os.environ,
        "RCLONE_CONFIG_MEGA_TYPE": "mega",
        "RCLONE_CONFIG_MEGA_USER": MEGA_USER,
        "RCLONE_CONFIG_MEGA_PASS": obscured,
    }


async def mega_upload(path: Path, tr: Tracker) -> Optional[str]:
    env_ = rclone_env()
    size = path.stat().st_size
    remote = f"MEGA:{MEGA_DIR.strip('/')}/{path.name}"
    proc = await asyncio.create_subprocess_exec(
        "rclone", "copyto", str(path), remote,
        "--use-json-log", "--stats", "2s", "--stats-log-level", "NOTICE", "--transfers", "1",
        stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.PIPE, env=env_,
    )
    last_err = ""
    async for raw in proc.stderr:
        line = raw.decode(errors="ignore").strip()
        try:
            obj = json.loads(line)
        except ValueError:
            continue
        if obj.get("level") in ("error", "critical"):
            last_err = str(obj.get("msg", ""))[:200]
        st = obj.get("stats")
        if st:
            await tr.update(int(st.get("bytes", 0)), int(st.get("totalBytes") or size))
    if await proc.wait() != 0:
        raise RuntimeError("MEGA আপলোড ব্যর্থ: " + (last_err or "rclone error"))
    await tr.update(size, size)
    link = await asyncio.create_subprocess_exec(
        "rclone", "link", remote, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL, env=env_
    )
    out, _ = await link.communicate()
    url = out.decode().strip()
    return url if url.startswith("http") else None


async def video_meta(path: Path, work: Path):
    duration = width = height = 0
    thumb = None
    try:
        p = await asyncio.create_subprocess_exec(
            "ffprobe", "-v", "error", "-select_streams", "v:0",
            "-show_entries", "stream=width,height:format=duration", "-of", "json", str(path),
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL,
        )
        out, _ = await p.communicate()
        j = json.loads(out or b"{}")
        st = (j.get("streams") or [{}])[0]
        width, height = int(st.get("width", 0)), int(st.get("height", 0))
        duration = int(float(j.get("format", {}).get("duration", 0)))
        t = work / "thumb.jpg"
        p = await asyncio.create_subprocess_exec(
            "ffmpeg", "-y", "-ss", str(min(3, duration // 2)), "-i", str(path),
            "-frames:v", "1", "-vf", "scale=320:-2", str(t),
            stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL,
        )
        await p.wait()
        if t.exists():
            thumb = str(t)
    except Exception:
        log.exception("video_meta failed")
    return duration, width, height, thumb


async def deliver(job: "Job", path: Path, work: Path, tr: Tracker):
    size = path.stat().st_size
    head = f"✅ <b>Task Completed</b>\n\n📄 <code>{esc(path.name)}</code>\n📦 {human(size)}\n"

    if job.dest == "tb":
        if not tb.ready:
            await tb.login()
        tr.set_stage("📤 TeraBox-এ আপলোড হচ্ছে…")
        res = await tb.upload(str(path), path.name, tr.update)
        links = await tb.share(res["path"])
        text = head + f"📂 <code>{esc(res['path'])}</code>\n"
        buttons = []
        if links["share"]:
            text += "\n🔗 শেয়ার লিংক তৈরি হয়েছে।"
            buttons.append([IKB("🔗 TeraBox Share Link", url=links["share"])])
        else:
            text += "\nℹ️ ফাইল আপনার TeraBox-এ সেভ হয়েছে। স্থায়ী শেয়ার লিংক তৈরি করা যায়নি।"
            if links["direct"]:
                buttons.append([IKB("⬇️ Direct link (সাময়িক)", url=links["direct"])])
            buttons.append([IKB("📂 TeraBox-এ খুলুন", url=tb.folder_link())])
        return text, buttons

    if job.dest == "gd":
        if not gd.ready:
            raise RuntimeError("Google Drive কনফিগার করা নেই")
        tr.set_stage("📤 Google Drive-এ আপলোড হচ্ছে…")
        res = await gd.upload(path, path.name, tr.update)
        return head, [[IKB("☁️ Drive-এ খুলুন", url=res["link"])]]

    if job.dest == "mg":
        if not (MEGA_USER and MEGA_PASS and shutil.which("rclone")):
            raise RuntimeError("MEGA কনফিগার করা নেই")
        tr.set_stage("📤 MEGA-তে আপলোড হচ্ছে…")
        link = await mega_upload(path, tr)
        return head, ([[IKB("Ⓜ️ MEGA লিংক", url=link)]] if link else [])

    if job.dest == "tg":
        if size > TG_LIMIT:
            raise RuntimeError("টেলিগ্রাম বট ২ GB-এর বেশি ফাইল পাঠাতে পারে না")
        tr.set_stage("📤 টেলিগ্রামে আপলোড হচ্ছে…")
        chat_id = job.msg.chat.id
        if path.suffix.lower() in VIDEO_EXT:
            dur, w, h, thumb = await video_meta(path, work)
            await app.send_video(
                chat_id, str(path), caption=esc(path.name), duration=dur, width=w, height=h,
                thumb=thumb, supports_streaming=True, progress=tr.update,
            )
        else:
            await app.send_document(chat_id, str(path), caption=esc(path.name), progress=tr.update)
        return head, []

    raise RuntimeError("অজানা গন্তব্য")


# ----------------------------------------------------------------------- queue
@dataclass
class Job:
    id: str
    dest: str
    kind: str
    name: str
    size: int
    url: str
    chat_id: int
    msg_id: int
    extra: dict
    msg: Message


async def process(job: Job):
    work = DOWNLOAD_DIR / job.id
    work.mkdir(parents=True, exist_ok=True)
    tr = Tracker(job.msg, job.name)
    try:
        path = await fetch_source(job, work, tr)
        tr.title = path.name
        text, buttons = await deliver(job, path, work, tr)
        await job.msg.edit_text(text, reply_markup=IKM(buttons) if buttons else None)
    finally:
        shutil.rmtree(work, ignore_errors=True)


async def worker():
    while True:
        job: Job = await QUEUE.get()
        try:
            await process(job)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.exception("job %s failed", job.id)
            try:
                await job.msg.edit_text(
                    f"❌ <b>ব্যর্থ</b>\n📄 {esc(job.name)}\n\n<code>{esc(str(e))[:400]}</code>"
                )
            except Exception:
                pass
        finally:
            QUEUE.task_done()


# -------------------------------------------------------------------- handlers
def new_pending(**data) -> str:
    if len(PENDING) > 300:
        PENDING.pop(next(iter(PENDING)))
    jid = uuid.uuid4().hex[:8]
    PENDING[jid] = data
    return jid


def keyboard(jid: str, kind: str) -> IKM:
    rows = [[IKB("📦 Send to TeraBox", callback_data=f"go:tb:{jid}")]]
    row = []
    if kind != "gdrive":
        row.append(IKB("☁️ Send to Drive", callback_data=f"go:gd:{jid}"))
    if kind != "mega":
        row.append(IKB("Ⓜ️ Send to MEGA", callback_data=f"go:mg:{jid}"))
    rows.append(row)
    if kind != "tg":
        rows.append([IKB("📥 Download to Telegram", callback_data=f"go:tg:{jid}")])
    return IKM(rows)


def panel(name: str, size: int, source: str) -> str:
    sz = f"\n📦 {human(size)}" if size else ""
    return f"📄 <b>{esc(name)}</b>{sz}\n🔎 সোর্স: {source}\n\nকোথায় পাঠাবেন?"


async def on_start(client, m: Message):
    await m.reply_text(HELP)


async def on_stranger(client, m: Message):
    await m.reply_text("⛔ এটি একটি প্রাইভেট বট।")


async def on_status(client, m: Message):
    quota = "—"
    if tb.ready:
        try:
            quota = await tb.quota_text()
        except Exception as e:
            quota = f"পড়া যায়নি ({type(e).__name__})"
    age = f"{(time.time() - tb.logged_at) / 3600:.1f} ঘণ্টা আগে" if tb.logged_at else "—"
    await m.reply_text(
        f"📊 কিউ: {QUEUE.qsize()}\n"
        f"🔐 TeraBox: {'✅ লগইন আছে' if tb.ready else '❌ লগইন নেই — /relogin'}\n"
        f"🕒 শেষ লগইন: {age} (অটো রিনিউ)\n"
        f"🗄 স্টোরেজ: {quota}\n"
        f"💾 খালি ডিস্ক: {human(shutil.disk_usage(DOWNLOAD_DIR).free)}"
    )


async def on_relogin(client, m: Message):
    try:
        await tb.login()
        await m.reply_text("✅ TeraBox-এ আবার লগইন সফল।")
    except Exception as e:
        await m.reply_text(f"❌ {esc(e)}")


async def on_media(client, m: Message):
    media = m.video or m.document or m.audio or m.animation or m.voice or m.video_note
    name = getattr(media, "file_name", None)
    if not name:
        ext = mimetypes.guess_extension(getattr(media, "mime_type", "") or "") or ""
        name = f"{media.file_unique_id}{ext}"
    size = int(getattr(media, "file_size", 0) or 0)
    jid = new_pending(kind="tg", name=name, size=size, url="", chat_id=m.chat.id, msg_id=m.id, extra={})
    await m.reply_text(panel(name, size, "Telegram"), reply_markup=keyboard(jid, "tg"), quote=True)


async def on_text(client, m: Message):
    urls = URL_RE.findall(m.text or "")
    if not urls:
        return await m.reply_text(HELP)
    for url in urls[:20]:
        kind = classify(url)
        if kind == "terabox":
            await m.reply_text(
                "ℹ️ TeraBox শেয়ার-লিংক থেকে ডাউনলোড অফিশিয়াল API-তে নেই, তাই সমর্থিত নয়। "
                "TeraBox এখানে শুধু <b>গন্তব্য</b> (Send to TeraBox)।"
            )
            continue
        name, size, extra = "Link", 0, {}
        if kind == "gdrive":
            try:
                fid = GDrive.parse_id(url)
                meta = await gd.info(fid)
                name, size, extra = meta["name"], int(meta.get("size", 0)), {"file_id": fid}
            except Exception as e:
                await m.reply_text(f"❌ {esc(e)}")
                continue
        elif kind == "mega":
            name = "MEGA ফাইল"
        else:
            name = unquote(os.path.basename(urlparse(url).path)) or "ডাইরেক্ট লিংক"
        label = {"gdrive": "Google Drive", "mega": "MEGA", "http": "Direct link"}[kind]
        jid = new_pending(kind=kind, name=name, size=size, url=url, chat_id=m.chat.id, msg_id=m.id, extra=extra)
        await m.reply_text(panel(name, size, label), reply_markup=keyboard(jid, kind), quote=True)


async def on_callback(client, cq):
    try:
        _, dest, jid = cq.data.split(":")
    except ValueError:
        return await cq.answer()
    p = PENDING.pop(jid, None)
    if not p:
        return await cq.answer("এই অনুরোধের মেয়াদ শেষ বা আগেই নেওয়া হয়েছে।", show_alert=True)
    job = Job(id=jid, dest=dest, msg=cq.message, **p)
    await QUEUE.put(job)
    await cq.message.edit_text(f"⏳ <b>কিউতে যোগ হয়েছে</b> (#{QUEUE.qsize()})\n📄 {esc(job.name)}")
    await cq.answer("কিউতে যোগ হয়েছে ✅")


# ------------------------------------------------------- web server / keep-alive
async def start_web():
    web_app = web.Application()

    async def health(_):
        return web.json_response({"ok": True, "queue": QUEUE.qsize()})

    web_app.router.add_get("/", health)
    web_app.router.add_get("/health", health)
    runner = web.AppRunner(web_app)
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", PORT).start()
    log.info("keep-alive server on :%s", PORT)


async def self_ping():
    base = env("RENDER_EXTERNAL_URL").rstrip("/")
    if not base:
        return
    async with aiohttp.ClientSession() as s:
        while True:
            await asyncio.sleep(540)  # Render free sleeps after 15 min of no inbound traffic
            try:
                async with s.get(base + "/health", timeout=aiohttp.ClientTimeout(total=20)) as r:
                    await r.read()
            except Exception as e:
                log.warning("self ping failed: %s", e)


async def session_keeper():
    while True:
        await asyncio.sleep(3600)
        try:
            await tb.maybe_relogin()
        except Exception as e:
            log.warning("session keeper: %s", e)


# ------------------------------------------------------------------------ main
async def main():
    global app, tb, gd, QUEUE
    if not (API_ID and API_HASH and BOT_TOKEN and OWNER_IDS):
        raise SystemExit("API_ID, API_HASH, BOT_TOKEN, OWNER_IDS সেট করুন")
    shutil.rmtree(DOWNLOAD_DIR, ignore_errors=True)
    DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
    QUEUE = asyncio.Queue()

    app = Client(
        "tgcloud", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN,
        in_memory=True, parse_mode=ParseMode.HTML, sleep_threshold=60,
    )
    media_f = filters.video | filters.document | filters.audio | filters.animation | filters.voice | filters.video_note
    cmds = ["start", "status", "relogin"]
    priv = filters.private & owner
    app.add_handler(MessageHandler(on_start, priv & filters.command(["start", "help"])))
    app.add_handler(MessageHandler(on_status, priv & filters.command("status")))
    app.add_handler(MessageHandler(on_relogin, priv & filters.command("relogin")))
    app.add_handler(MessageHandler(on_media, priv & media_f))
    app.add_handler(MessageHandler(on_text, priv & filters.text & ~filters.command(cmds + ["help"])))
    app.add_handler(MessageHandler(on_stranger, filters.private & ~owner))
    app.add_handler(CallbackQueryHandler(on_callback, filters.regex(r"^go:") & owner))

    await app.start()
    gd = GDrive()
    tb = TeraBox(
        env("TERABOX_EMAIL"), env("TERABOX_PASSWORD"),
        remote_dir=env("TERABOX_REMOTE_DIR", "/TelegramBot"),
    )
    try:
        await tb.login()
        tb_note = "✅ TeraBox লগইন সফল"
    except Exception as e:
        log.exception("TeraBox login failed")
        tb_note = f"⚠️ {esc(e)}"

    await start_web()
    tasks = [asyncio.create_task(worker()), asyncio.create_task(self_ping()), asyncio.create_task(session_keeper())]
    for oid in OWNER_IDS:
        try:
            await app.send_message(oid, f"🟢 Bot online\n{tb_note}")
        except Exception:
            pass
    log.info("bot started")
    await idle()
    for t in tasks:
        t.cancel()
    await tb.close()
    await gd.close()
    await app.stop()


if __name__ == "__main__":
    asyncio.run(main())
