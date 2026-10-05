"""TeraBox module: permanent auto-login with EMAIL + PASSWORD (no ndus cookie to copy).

How "permanent" works
  * The bot logs in by itself (aioterabox) and builds fresh session cookies + jsToken.
  * If anything fails (expired session, 4xx/5xx, bad token) it logs in again and retries once.
  * It also re-logs-in proactively every `relogin_hours`.
Real upload progress: we count the bytes actually sent over the wire with an aiohttp
trace hook, so it does not depend on aioterabox internals.
"""
import asyncio
import inspect
import json
import logging
import os
import re
import time
from typing import Awaitable, Callable, Optional
from urllib.parse import quote

import aiohttp
from aioterabox.api import TeraboxClient
from aioterabox.exceptions import TeraboxLoginChallengeRequired
from yarl import URL

log = logging.getLogger("terabox")

BASE = "https://www.terabox.com"
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
Progress = Callable[[int, int], Awaitable[None]]


class TeraBoxError(Exception):
    pass


COOKIE_FILE = "/tmp/tb_cookies.json"
_SECRET = re.compile(r'("?[\w-]*(?:token|ndus|bduss|stoken|cookie|pass|sign)[\w-]*"?\s*[:=]\s*"?)([^",&\s}]{4,})', re.I)


def _redact(text: str) -> str:
    return _SECRET.sub(lambda m: m.group(1) + "***", text)


def parse_cookies(raw: str) -> dict:
    """Accepts a bare ndus value, or 'k=v; k=v' (browser Cookie header format)."""
    raw = (raw or "").strip()
    if not raw:
        return {}
    if "=" not in raw:
        return {"ndus": raw}
    out = {}
    for part in re.split(r";\s*", raw):
        if "=" in part:
            k, v = part.split("=", 1)
            if k.strip() and v.strip():
                out[k.strip()] = v.strip()
    return out


class TeraBox:
    def __init__(self, email: str = "", password: str = "", remote_dir: str = "/TelegramBot",
                 relogin_hours: float = 20, cookies: str = ""):
        self.email = email
        self.password = password
        self.remote_dir = "/" + remote_dir.strip("/")
        self.relogin_after = relogin_hours * 3600
        self.client: Optional[TeraboxClient] = None
        self.session: Optional[aiohttp.ClientSession] = None
        self.logged_at = 0.0
        self._dir_ok = False
        self._sent = 0
        self._counting = False
        self._lock = asyncio.Lock()
        self._debug = False
        self.mode = ""
        self.cookies = parse_cookies(cookies)
        try:
            with open(COOKIE_FILE, encoding="utf-8") as f:  # newer cookies set via /setcookie win
                saved = json.load(f)
            if isinstance(saved, dict) and saved:
                self.cookies = saved
        except Exception:
            pass

    # ------------------------------------------------------------------ login
    @property
    def ready(self) -> bool:
        return self.client is not None and self.logged_at > 0

    def _new_session(self) -> aiohttp.ClientSession:
        trace = aiohttp.TraceConfig()

        async def on_chunk(session, ctx, params):
            if self._counting:
                self._sent += len(params.chunk)

        async def on_end(session, ctx, params):
            if self._debug:
                log.info("[tb-debug] %s %s%s -> %s", params.method, params.url.host, params.url.path,
                         params.response.status)

        async def on_body(session, ctx, params):
            if self._debug and params.chunk:
                log.info("[tb-debug] body: %s", _redact(params.chunk[:400].decode("utf-8", "ignore")))

        trace.on_request_chunk_sent.append(on_chunk)
        trace.on_request_end.append(on_end)
        trace.on_response_chunk_received.append(on_body)
        return aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=None, sock_connect=30, sock_read=300),
            trace_configs=[trace],
        )

    def set_cookies(self, raw: str) -> list:
        """Hot-swap cookies (used by /setcookie). Returns the cookie names stored."""
        parsed = parse_cookies(raw)
        if not parsed:
            raise TeraBoxError("কুকি পড়া যায়নি")
        self.cookies = parsed
        try:
            with open(COOKIE_FILE, "w", encoding="utf-8") as f:
                json.dump(parsed, f)
        except Exception:
            log.warning("could not persist cookies to %s", COOKIE_FILE)
        return sorted(parsed)

    async def _login_once(self, mode: str):
        if self.session and not self.session.closed:
            await self.session.close()
        self.session = self._new_session()
        kwargs = {"session": self.session}
        params = set(inspect.signature(TeraboxClient.__init__).parameters) - {"self", "session"}
        if mode == "password":
            kwargs.update(email=self.email, password=self.password)
        else:
            norm = {re.sub(r"[_\-]", "", k.lower()): v for k, v in self.cookies.items()}
            for p in params:
                key = re.sub(r"[_\-]", "", p.lower())
                if key in norm:
                    kwargs[p] = norm[key]
            if "cookies" in params:
                kwargs["cookies"] = dict(self.cookies)
            self.session.cookie_jar.update_cookies(self.cookies, URL(BASE))
        self.client = TeraboxClient(**kwargs)
        self._dir_ok = False
        self._debug = True
        try:
            try:
                res = await self.client.login()
            except TeraboxLoginChallengeRequired as exc:
                res = await self.client.complete_login_challenge(exc.challenge)
        finally:
            self._debug = False
        if mode == "cookies" and isinstance(res, dict):  # keep any refreshed cookies
            fresh = {k: v for k, v in res.items() if isinstance(v, str) and v}
            if fresh:
                self.set_cookies("; ".join(f"{k}={v}" for k, v in {**self.cookies, **fresh}.items()))

    async def login(self):
        modes = []
        if self.cookies:
            modes.append("cookies")
        if self.email and self.password:
            modes.append("password")
        if not modes:
            raise TeraBoxError("কুকি বা ইমেইল-পাসওয়ার্ড — কিছুই সেট করা নেই (/setcookie দিন)")
        async with self._lock:
            errors = []
            for mode in modes:
                try:
                    await self._login_once(mode)
                    self.logged_at, self.mode = time.time(), mode
                    log.info("TeraBox logged in via %s", mode)
                    return
                except Exception as e:
                    log.exception("TeraBox login via %s failed", mode)
                    errors.append(f"{mode}: {type(e).__name__}: {str(e)[:150]}")
            self.logged_at = 0.0
            raise TeraBoxError("TeraBox লগইন ব্যর্থ — " + " | ".join(errors))

    async def maybe_relogin(self):
        if self.email and (not self.ready or time.time() - self.logged_at > self.relogin_after):
            await self.login()

    async def _with_retry(self, fn):
        """Run fn() (returns an awaitable). On any error: re-login once, retry once."""
        await self.maybe_relogin()
        try:
            return await fn()
        except TeraBoxError:
            raise
        except Exception as e:
            log.warning("TeraBox op failed (%r) -> re-login and retry once", e)
            await self.login()
            return await fn()

    async def close(self):
        if self.session and not self.session.closed:
            await self.session.close()

    # ------------------------------------------------------------------ helpers
    async def _ensure_dir(self, c):
        if self._dir_ok:
            return
        try:
            await c.create_directory(self.remote_dir)
        except Exception:
            pass  # most likely "already exists"
        self._dir_ok = True

    async def _unique_name(self, c, name: str) -> str:
        try:
            items = await c.list_remote_directory(self.remote_dir)
            existing = set()
            for i in items or []:
                if isinstance(i, dict):
                    n = i.get("server_filename") or i.get("name") or i.get("path") or ""
                    existing.add(str(n).split("/")[-1])
        except Exception:
            return name
        if name not in existing:
            return name
        stem, ext = os.path.splitext(name)
        n = 1
        while f"{stem}_{n}{ext}" in existing:
            n += 1
        return f"{stem}_{n}{ext}"

    async def _poll(self, size: int, progress: Progress):
        while True:
            await asyncio.sleep(2)
            await progress(min(self._sent, size), size)

    # ------------------------------------------------------------------- upload
    async def upload(self, local_path: str, remote_name: Optional[str] = None,
                     progress: Optional[Progress] = None) -> dict:
        """Upload into remote_dir. Returns {"path": "/TelegramBot/name", "size": n}."""
        size = os.path.getsize(local_path)
        if size <= 0:
            raise TeraBoxError("ফাইল খালি")
        name = (remote_name or os.path.basename(local_path)).replace("/", "_")

        async def attempt():
            c = self.client
            await self._ensure_dir(c)
            remote = f"{self.remote_dir}/{await self._unique_name(c, name)}"
            self._sent = 0
            self._counting = True
            poller = asyncio.create_task(self._poll(size, progress)) if progress else None
            try:
                await c.upload_file(local_path, remote)
            finally:
                self._counting = False
                if poller:
                    poller.cancel()
            return remote

        remote = await self._with_retry(attempt)
        if progress:
            await progress(size, size)
        return {"path": remote, "size": size}

    # -------------------------------------------------------------------- links
    def _js_token(self) -> str:
        c = self.client
        for n in ("js_token", "jstoken", "jsToken", "_js_token", "_jstoken"):
            v = getattr(c, n, None)
            if isinstance(v, str) and v:
                return v
        for k, v in vars(c).items():
            if "js" in k.lower() and "token" in k.lower() and isinstance(v, str) and v:
                return v
        for v in vars(c).values():
            if isinstance(v, dict):
                for kk, vv in v.items():
                    if str(kk).lower() in ("jstoken", "js_token") and isinstance(vv, str) and vv:
                        return vv
        return ""

    async def _try_share(self, fs_id) -> Optional[str]:
        params = {"app_id": "250528", "web": "1", "channel": "dubox", "clienttype": "0", "jsToken": self._js_token()}
        headers = {"User-Agent": UA, "Referer": f"{BASE}/main", "X-Requested-With": "XMLHttpRequest"}
        attempts = [
            ("/share/pset", {"fid_list": f"[{fs_id}]", "schannel": "0", "channel_list": "[]", "period": "0", "public": "1"}),
            ("/share/set", {"fid_list": f"[{fs_id}]", "schannel": "0", "channel_list": "[]", "period": "0"}),
        ]
        for path, data in attempts:
            try:
                async with self.session.post(BASE + path, params=params, data=data, headers=headers) as r:
                    j = await r.json(content_type=None)
                if isinstance(j, dict) and j.get("errno", 0) == 0:
                    link = j.get("link") or j.get("shorturl")
                    if link:
                        return link if str(link).startswith("http") else f"{BASE}/s/{link}"
            except Exception as e:
                log.warning("share via %s failed: %s", path, e)
        return None

    async def share(self, remote_path: str) -> dict:
        """Returns {"share": permanent share link or None, "direct": temporary signed link or None}."""
        out = {"share": None, "direct": None}
        try:
            metas = await self._with_retry(lambda: self.client.get_files_meta([remote_path]))
            meta = metas[0] if metas else None
        except Exception as e:
            log.warning("get_files_meta failed: %s", e)
            return out
        if not isinstance(meta, dict):
            return out
        out["direct"] = meta.get("dlink")
        fs_id = meta.get("fs_id") or meta.get("fsid")
        if fs_id:
            out["share"] = await self._try_share(fs_id)
        return out

    def folder_link(self) -> str:
        return f"{BASE}/main?category=all&path={quote(self.remote_dir)}"

    async def quota_text(self) -> str:
        q = await self._with_retry(lambda: self.client.get_storage_quota())
        if isinstance(q, dict) and "total" in q and "used" in q:
            gb = 1024 ** 3
            return f"{int(q['used']) / gb:.1f} / {int(q['total']) / gb:.1f} GB"
        return str(q)[:120]
