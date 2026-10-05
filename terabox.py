"""TeraBox module: permanent auto-login with EMAIL + PASSWORD (no ndus cookie to copy).

How "permanent" works
  * The bot logs in by itself (aioterabox) and builds fresh session cookies + jsToken.
  * If anything fails (expired session, 4xx/5xx, bad token) it logs in again and retries once.
  * It also re-logs-in proactively every `relogin_hours`.
Real upload progress: we count the bytes actually sent over the wire with an aiohttp
trace hook, so it does not depend on aioterabox internals.
"""
import asyncio
import logging
import os
import time
from typing import Awaitable, Callable, Optional
from urllib.parse import quote

import aiohttp
from aioterabox.api import TeraboxClient
from aioterabox.exceptions import TeraboxLoginChallengeRequired

log = logging.getLogger("terabox")

BASE = "https://www.terabox.com"
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
Progress = Callable[[int, int], Awaitable[None]]


class TeraBoxError(Exception):
    pass


class TeraBox:
    def __init__(self, email: str, password: str, remote_dir: str = "/TelegramBot", relogin_hours: float = 20):
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

    # ------------------------------------------------------------------ login
    @property
    def ready(self) -> bool:
        return self.client is not None and self.logged_at > 0

    def _new_session(self) -> aiohttp.ClientSession:
        trace = aiohttp.TraceConfig()

        async def on_chunk(session, ctx, params):
            if self._counting:
                self._sent += len(params.chunk)

        trace.on_request_chunk_sent.append(on_chunk)
        return aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=None, sock_connect=30, sock_read=300),
            trace_configs=[trace],
        )

    async def login(self):
        if not (self.email and self.password):
            raise TeraBoxError("TERABOX_EMAIL / TERABOX_PASSWORD সেট করা নেই")
        async with self._lock:
            if self.session and not self.session.closed:
                await self.session.close()
            self.session = self._new_session()
            self.client = TeraboxClient(session=self.session, email=self.email, password=self.password)
            self._dir_ok = False
            try:
                try:
                    await self.client.login()
                except TeraboxLoginChallengeRequired as exc:
                    # TeraBox asked for the "simple verify" continuation step -> do it automatically
                    await self.client.complete_login_challenge(exc.challenge)
            except Exception as e:
                self.logged_at = 0.0
                raise TeraBoxError(
                    f"TeraBox লগইন ব্যর্থ: {type(e).__name__}: {str(e)[:200]} "
                    "(ক্যাপচা/ভেরিফিকেশন লাগতে পারে — অ্যাপে একবার লগইন করে দেখুন)"
                ) from e
            self.logged_at = time.time()
            log.info("TeraBox logged in")

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
