"""Google Drive helper (REST + aiohttp, streaming, low RAM).

Credentials (first match wins):
  1. OAuth user refresh token : GDRIVE_CLIENT_ID / GDRIVE_CLIENT_SECRET / GDRIVE_REFRESH_TOKEN
  2. Service account JSON     : GDRIVE_SERVICE_ACCOUNT_JSON (single-line JSON string)
Upload to a personal "My Drive" needs (1) (service accounts have no storage quota there);
(2) works for downloads and for uploads into a Shared Drive folder.
"""
import asyncio
import json
import logging
import os
import re
from pathlib import Path
from typing import Awaitable, Callable, Optional

import aiohttp
from google.auth.transport.requests import Request
from google.oauth2 import service_account
from google.oauth2.credentials import Credentials

log = logging.getLogger("gdrive")

SCOPES = ["https://www.googleapis.com/auth/drive"]
API = "https://www.googleapis.com/drive/v3"
UPLOAD = "https://www.googleapis.com/upload/drive/v3/files"
CHUNK = 8 * 1024 * 1024  # must be a multiple of 256 KiB
Progress = Callable[[int, int], Awaitable[None]]


class GDriveError(Exception):
    pass


class GDrive:
    def __init__(self):
        self.folder_id = os.getenv("GDRIVE_FOLDER_ID", "").strip()
        self.creds = self._load_creds()
        self._session: Optional[aiohttp.ClientSession] = None

    # ------------------------------------------------------------------ setup
    @staticmethod
    def _load_creds():
        cid = os.getenv("GDRIVE_CLIENT_ID", "").strip()
        sec = os.getenv("GDRIVE_CLIENT_SECRET", "").strip()
        rt = os.getenv("GDRIVE_REFRESH_TOKEN", "").strip()
        if cid and sec and rt:
            return Credentials(
                None,
                refresh_token=rt,
                token_uri="https://oauth2.googleapis.com/token",
                client_id=cid,
                client_secret=sec,
                scopes=SCOPES,
            )
        raw = os.getenv("GDRIVE_SERVICE_ACCOUNT_JSON", "").strip()
        if raw:
            return service_account.Credentials.from_service_account_info(json.loads(raw), scopes=SCOPES)
        return None

    @property
    def ready(self) -> bool:
        return self.creds is not None

    def _sess(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=None, sock_connect=30, sock_read=180)
            )
        return self._session

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()

    def _token_sync(self) -> str:
        if not self.creds.valid:
            self.creds.refresh(Request())
        return self.creds.token

    async def _headers(self) -> dict:
        if not self.ready:
            raise GDriveError("Google Drive কনফিগার করা নেই")
        return {"Authorization": "Bearer " + await asyncio.to_thread(self._token_sync)}

    # ---------------------------------------------------------------- helpers
    @staticmethod
    def parse_id(url: str) -> str:
        if "/folders/" in url or "/drive/folders" in url:
            raise GDriveError("ফোল্ডার লিংক সমর্থিত নয় — একটি ফাইলের লিংক দিন")
        m = re.search(r"/d/([\w-]{10,})", url) or re.search(r"[?&]id=([\w-]{10,})", url)
        if not m:
            raise GDriveError("Drive লিংক থেকে ফাইল ID পাওয়া যায়নি")
        return m.group(1)

    async def info(self, file_id: str) -> dict:
        h = await self._headers()
        params = {"fields": "id,name,size,mimeType", "supportsAllDrives": "true"}
        async with self._sess().get(f"{API}/files/{file_id}", params=params, headers=h) as r:
            if r.status == 404:
                raise GDriveError("ফাইল পাওয়া যায়নি বা অ্যাক্সেস নেই (ফাইলটি Service Account-এর ইমেইলের সাথে শেয়ার করুন)")
            if r.status != 200:
                raise GDriveError(f"Drive error {r.status}: {(await r.text())[:200]}")
            return await r.json()

    # --------------------------------------------------------------- download
    async def download(self, file_id: str, dest_dir: Path, progress: Optional[Progress] = None) -> Path:
        meta = await self.info(file_id)
        if meta.get("mimeType", "").startswith("application/vnd.google-apps"):
            raise GDriveError("Google Docs/Sheets/Folder ডাউনলোড সমর্থিত নয় — শুধু সাধারণ ফাইল")
        total = int(meta.get("size", 0))
        name = re.sub(r"[\\/:*?\"<>|]", "_", meta["name"]) or file_id
        path = Path(dest_dir) / name
        h = await self._headers()
        params = {"alt": "media", "supportsAllDrives": "true"}
        cur = 0
        async with self._sess().get(f"{API}/files/{file_id}", params=params, headers=h) as r:
            if r.status != 200:
                raise GDriveError(f"ডাউনলোড ব্যর্থ ({r.status}): {(await r.text())[:200]}")
            with open(path, "wb") as f:
                async for chunk in r.content.iter_chunked(1 << 20):
                    f.write(chunk)
                    cur += len(chunk)
                    if progress:
                        await progress(cur, total)
        return path

    # ----------------------------------------------------------------- upload
    async def upload(self, path: Path, name: str, progress: Optional[Progress] = None) -> dict:
        size = os.path.getsize(path)
        if size <= 0:
            raise GDriveError("ফাইল খালি")
        meta = {"name": name}
        if self.folder_id:
            meta["parents"] = [self.folder_id]
        h = await self._headers()
        init_headers = {
            **h,
            "X-Upload-Content-Length": str(size),
            "Content-Type": "application/json; charset=UTF-8",
        }
        async with self._sess().post(
            UPLOAD,
            params={"uploadType": "resumable", "supportsAllDrives": "true"},
            headers=init_headers,
            data=json.dumps(meta),
        ) as r:
            if r.status != 200:
                raise GDriveError(f"আপলোড শুরু ব্যর্থ ({r.status}): {(await r.text())[:300]}")
            session_url = r.headers["Location"]

        sent = 0
        result = None
        with open(path, "rb") as f:
            while sent < size:
                chunk = await asyncio.to_thread(f.read, CHUNK)
                end = sent + len(chunk) - 1
                headers = {"Content-Length": str(len(chunk)), "Content-Range": f"bytes {sent}-{end}/{size}"}
                ok = False
                for attempt in range(1, 6):
                    try:
                        async with self._sess().put(
                            session_url, data=chunk, headers=headers, allow_redirects=False
                        ) as r:
                            if r.status in (200, 201):
                                result = await r.json()
                                ok = True
                            elif r.status == 308:
                                ok = True
                            elif r.status >= 500:
                                raise aiohttp.ClientError(f"server {r.status}")
                            else:
                                raise GDriveError(f"আপলোড ব্যর্থ ({r.status}): {(await r.text())[:300]}")
                    except (aiohttp.ClientError, asyncio.TimeoutError):
                        await asyncio.sleep(2 ** attempt)
                        continue
                    break
                if not ok:
                    raise GDriveError("আপলোড বারবার ব্যর্থ হয়েছে")
                sent += len(chunk)
                if progress:
                    await progress(sent, size)
        if not result or "id" not in result:
            raise GDriveError("আপলোড শেষ হয়নি")
        return {"id": result["id"], "link": f"https://drive.google.com/file/d/{result['id']}/view"}
