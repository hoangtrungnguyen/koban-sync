"""Capture images from Zalo chats (zlapi) and log them to a CSV file.

Two modes:

    listen    sync anything pending, then grab every image as it arrives
    history   backfill from the little history zalo keeps server-side
    chats     print every chat (groups + friends) with its id
    whois     look up a person by phone number or user id

Pick chats with --chat "Flutter Team" (name or id); default is every chat.

Every captured image is downloaded into --out-dir and appended as one row to
--csv. Rows are de-duplicated on (msg_id, url), so re-running never duplicates
work and an interrupted run can simply be restarted.
"""

import argparse
import csv
import hashlib
import json
import mimetypes
import os
import re
import signal
import subprocess
import sys
import threading
import time
import unicodedata
from datetime import datetime, timezone
from urllib.parse import urlparse

import requests
from zlapi import ZaloAPI, ThreadType

# --------------------------------------------------------------------------
# message parsing
# --------------------------------------------------------------------------

# msgType values zalo uses for attachments that carry a picture
PHOTO_TYPES = {"chat.photo", "chat.gif", "chat.sticker.photo"}
VIDEO_TYPES = {"chat.video.msg"}          # only a poster/thumbnail is available
FILE_TYPES = {"share.file", "chat.attachment"}

IMAGE_EXTS = {".jpg", ".jpeg", ".png", ".gif", ".webp", ".bmp", ".heic", ".tiff"}

CSV_FIELDS = [
    "captured_at",
    "msg_id",
    "cli_msg_id",
    "sent_at",
    "ts_ms",
    "thread_id",
    "thread_type",
    "thread_name",
    "sender_id",
    "sender_name",
    "msg_type",
    "caption",
    "url",
    "file_name",
    "saved_path",
    "width",
    "height",
    "file_size",
    "sha256",
    "album_id",
    "album_index",
    "album_total",
    "status",
]


def _as_dict(value):
    """zlapi wraps payloads in Munch objects; content is sometimes a JSON str."""
    if value is None:
        return {}
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except (ValueError, TypeError):
            return {}
    if isinstance(value, dict):
        return dict(value)
    if hasattr(value, "toDict"):
        return value.toDict()
    return {}


def _looks_like_image(url, title=""):
    path = urlparse(url or "").path.lower()
    ext = os.path.splitext(path)[1]
    if ext in IMAGE_EXTS:
        return True
    return os.path.splitext((title or "").lower())[1] in IMAGE_EXTS


def extract_images(message_object, include_video_thumbs=False):
    """Return a list of image URLs found in a zalo message.

    Highest quality first: original -> hd -> normal -> href -> thumbnail.
    """
    msg_type = message_object.get("msgType") or ""
    content = _as_dict(message_object.get("content"))
    params = _as_dict(content.get("params"))

    if msg_type in PHOTO_TYPES:
        pass
    elif msg_type in VIDEO_TYPES:
        if not include_video_thumbs:
            return []
    elif msg_type in FILE_TYPES:
        if not _looks_like_image(content.get("href"), content.get("title")):
            return []
    else:
        return []

    urls = []
    best = _best_url(content, params)
    if best:
        urls.append(best)

    # a multi-photo message keeps its siblings in content["items"]
    for item in content.get("items") or []:
        item = _as_dict(item)
        sibling = _best_url(item, _as_dict(item.get("params")))
        if sibling and sibling not in urls:
            urls.append(sibling)

    return urls


def _best_url(content, params=None):
    """Pick the highest quality link available: original > hd > normal > thumb."""
    params = params or {}
    for source in (params, content):
        for key in ("oriUrl", "hdUrl", "normalUrl", "href", "thumb", "thumbUrl"):
            url = source.get(key)
            if isinstance(url, str) and url.startswith("http"):
                return url
    return None


def iso_from_ts(ts):
    try:
        ts = int(ts)
    except (TypeError, ValueError):
        return ""
    if ts > 10_000_000_000:  # milliseconds
        ts = ts / 1000
    return datetime.fromtimestamp(ts, tz=timezone.utc).astimezone().isoformat()


# --------------------------------------------------------------------------
# upstream fix
# --------------------------------------------------------------------------

# zlapi 1.0.3 builds msgObj *after* the group loop, so a websocket frame that
# carries several group messages - exactly what sending several images at once
# produces - only ever delivers its last message. Move the dispatch inside the
# loop. Patched at runtime: if a later zlapi rewrites that code the source stops
# matching and the library is left untouched.
_BUGGY_GROUP_DISPATCH = '\t\t\t\t\tgroupMsgs = parsedData["data"]["groupMsgs"]\n\t\t\t\t\t\n\t\t\t\t\ttry:\n\t\t\t\t\t\tfor message in groupMsgs:\n\t\t\t\t\t\t\tmessages = self.getRecentGroup(message["idTo"])["groupMsgs"]\n\t\t\t\t\t\t\tmessage = next((msg for msg in messages if msg["msgId"] == message["msgId"]), message)\n\t\t\t\t\texcept:\n\t\t\t\t\t\tpass\n\t\t\t\t\t\t\n\t\t\t\t\tmsgObj = MessageObject.fromDict(message, None)\n'
_FIXED_GROUP_DISPATCH = '\t\t\t\t\tgroupMsgs = parsedData["data"]["groupMsgs"]\n\t\t\t\t\t\n\t\t\t\t\tfor message in groupMsgs:\n\t\t\t\t\t\ttry:\n\t\t\t\t\t\t\tmessages = self.getRecentGroup(message["idTo"])["groupMsgs"]\n\t\t\t\t\t\t\tmessage = next((msg for msg in messages if msg["msgId"] == message["msgId"]), message)\n\t\t\t\t\t\texcept:\n\t\t\t\t\t\t\tpass\n\t\t\t\t\t\t\n\t\t\t\t\t\tmsgObj = MessageObject.fromDict(message, None)\n'
_GROUP_DISPATCH = '\t\t\t\t\t[\n\t\t\t\t\t\tpool.submit(self.onMessage, msgObj.msgId, str(int(msgObj.uidFrom) or self.user_id), msgObj.content, msgObj, str(int(msgObj.idTo) or self.user_id), ThreadType.GROUP)\n\t\t\t\t\t\tif self.thread else\n\t\t\t\t\t\tself.onMessage(msgObj.msgId, str(int(msgObj.uidFrom) or self.user_id), msgObj.content, msgObj, str(int(msgObj.idTo) or self.user_id), ThreadType.GROUP)\n\t\t\t\t\t]\n'
_GROUP_DISPATCH_INDENTED = '\t\t\t\t\t\t[\n\t\t\t\t\t\t\tpool.submit(self.onMessage, msgObj.msgId, str(int(msgObj.uidFrom) or self.user_id), msgObj.content, msgObj, str(int(msgObj.idTo) or self.user_id), ThreadType.GROUP)\n\t\t\t\t\t\t\tif self.thread else\n\t\t\t\t\t\t\tself.onMessage(msgObj.msgId, str(int(msgObj.uidFrom) or self.user_id), msgObj.content, msgObj, str(int(msgObj.idTo) or self.user_id), ThreadType.GROUP)\n\t\t\t\t\t\t]\n'


_PATCHED_SOURCE = None


def patch_multi_message_delivery():
    """Rebind a corrected `_listen`. Returns True when the patch applied."""
    import inspect
    import textwrap
    import zlapi._client as zc

    try:
        source = inspect.getsource(zc.ZaloAPI._listen)
    except (OSError, TypeError):
        return False

    if _BUGGY_GROUP_DISPATCH not in source or _GROUP_DISPATCH not in source:
        return False

    fixed = source.replace(_BUGGY_GROUP_DISPATCH, _FIXED_GROUP_DISPATCH)
    fixed = fixed.replace(_GROUP_DISPATCH, _GROUP_DISPATCH_INDENTED, 1)

    namespace = dict(vars(zc))
    try:
        exec(compile(textwrap.dedent(fixed), zc.__file__, "exec"), namespace)
        zc.ZaloAPI._listen = namespace["_listen"]
    except Exception:
        return False

    global _PATCHED_SOURCE
    _PATCHED_SOURCE = textwrap.dedent(fixed)
    return True


# --------------------------------------------------------------------------
# storage
# --------------------------------------------------------------------------

def _safe(name, fallback="x"):
    name = re.sub(r"[^A-Za-z0-9._-]+", "_", str(name or "")).strip("._")
    return name[:80] or fallback


class ImageStore:
    """Downloads images to disk and appends one CSV row per image."""

    def __init__(self, csv_path, out_dir, cookies=None, user_agent=None):
        self.csv_path = csv_path
        self.out_dir = out_dir
        self.lock = threading.Lock()
        self.seen = set()
        self.albums = {}          # album_id -> set of captured indexes

        os.makedirs(out_dir, exist_ok=True)
        parent = os.path.dirname(os.path.abspath(csv_path))
        os.makedirs(parent, exist_ok=True)

        self.session = requests.Session()
        if cookies:
            self.session.cookies.update(cookies)
        self.session.headers.update({
            "User-Agent": user_agent or "Mozilla/5.0",
            "Referer": "https://chat.zalo.me/",
        })

        self._migrate_header(csv_path)
        is_new = not os.path.exists(csv_path) or os.path.getsize(csv_path) == 0
        self._fh = open(csv_path, "a", newline="", encoding="utf-8-sig")
        self._writer = csv.DictWriter(self._fh, fieldnames=CSV_FIELDS)
        if is_new:
            self._writer.writeheader()
            self._fh.flush()
        else:
            self._load_seen()

    @staticmethod
    def _migrate_header(csv_path):
        """Rewrite an older CSV in place when new columns are added."""
        if not os.path.exists(csv_path) or os.path.getsize(csv_path) == 0:
            return
        with open(csv_path, "r", newline="", encoding="utf-8-sig") as fh:
            reader = csv.DictReader(fh)
            if reader.fieldnames == CSV_FIELDS:
                return
            rows = list(reader)

        with open(csv_path, "w", newline="", encoding="utf-8-sig") as fh:
            writer = csv.DictWriter(fh, fieldnames=CSV_FIELDS)
            writer.writeheader()
            for row in rows:
                writer.writerow({k: row.get(k, "") or "" for k in CSV_FIELDS})
        print(f"migrated {csv_path} to the current columns")

    def _load_seen(self):
        """Remember what is already captured; failed rows stay retryable."""
        with open(self.csv_path, "r", newline="", encoding="utf-8-sig") as fh:
            for row in csv.DictReader(fh):
                if (row.get("status") or "").startswith("download_error"):
                    continue
                self.seen.add((row.get("msg_id"), row.get("url")))
                if row.get("album_id"):
                    self.albums.setdefault(row["album_id"], set()).add(
                        row.get("album_index") or "")

    def close(self):
        try:
            self._fh.close()
        except Exception:
            pass

    def already_have(self, msg_id, url):
        with self.lock:
            return (str(msg_id), url) in self.seen

    def _download(self, url, base_name):
        resp = self.session.get(url, timeout=30, stream=True)
        resp.raise_for_status()

        ext = os.path.splitext(urlparse(url).path)[1].lower()
        if ext not in IMAGE_EXTS:
            ctype = (resp.headers.get("Content-Type") or "").split(";")[0].strip()
            ext = mimetypes.guess_extension(ctype) or ".jpg"
            ext = ".jpg" if ext == ".jpe" else ext

        path = os.path.join(self.out_dir, base_name + ext)
        counter = 1
        while os.path.exists(path):
            path = os.path.join(self.out_dir, f"{base_name}_{counter}{ext}")
            counter += 1

        digest = hashlib.sha256()
        size = 0
        with open(path, "wb") as fh:
            for chunk in resp.iter_content(64 * 1024):
                if not chunk:
                    continue
                fh.write(chunk)
                digest.update(chunk)
                size += len(chunk)

        return path, size, digest.hexdigest()

    @staticmethod
    def _dimensions(path):
        try:
            from PIL import Image  # optional dependency
        except ImportError:
            return "", ""
        try:
            with Image.open(path) as img:
                return img.width, img.height
        except Exception:
            return "", ""

    def save(self, meta, url, download=True):
        """Download one image (if new) and append its CSV row."""
        if self.already_have(meta["msg_id"], url):
            return None

        row = dict.fromkeys(CSV_FIELDS, "")
        row.update(meta)
        row["url"] = url
        row["captured_at"] = datetime.now().astimezone().isoformat(timespec="seconds")

        base = f"{meta.get('ts_ms') or int(time.time() * 1000)}_{_safe(meta.get('thread_id'))}_{_safe(meta.get('msg_id'))}"
        if download:
            try:
                path, size, sha = self._download(url, base)
                width, height = self._dimensions(path)
                row.update({
                    "saved_path": path,
                    "file_name": os.path.basename(path),
                    "file_size": size,
                    "sha256": sha,
                    "width": width,
                    "height": height,
                    "status": "ok",
                })
            except Exception as exc:
                row["status"] = f"download_error: {exc}"
        else:
            row["status"] = "url_only"

        with self.lock:
            if (str(meta["msg_id"]), url) in self.seen:
                return None
            self.seen.add((str(meta["msg_id"]), url))
            self._writer.writerow(row)
            self._fh.flush()

            album_id = row.get("album_id")
            if album_id:
                members = self.albums.setdefault(album_id, set())
                members.add(str(row.get("album_index") or ""))
                row["_album_have"] = len(members)

        return row


# --------------------------------------------------------------------------
# zalo client
# --------------------------------------------------------------------------

def _find_name(payload, thread_id):
    """Dig the display name out of a fetchGroupInfo/fetchUserInfo response."""
    payload = _as_dict(payload)
    containers = ["gridInfoMap", "changed_profiles", "unchanged_profiles", "profiles"]
    for key in containers:
        entry = _as_dict((payload.get(key) or {}).get(str(thread_id)))
        for name_key in ("name", "displayName", "zaloName", "username"):
            if entry.get(name_key):
                return entry[name_key]
    for name_key in ("name", "displayName", "zaloName"):
        if payload.get(name_key):
            return payload[name_key]
    return ""


class ImageCaptureBot(ZaloAPI):
    """Listens on the account and hands every image message to the store."""

    def configure(self, store, args):
        self.store = store
        self.args = args
        self._names = {}
        return self

    # -- helpers ----------------------------------------------------------
    def thread_name(self, thread_id, thread_type):
        """Best-effort display name for a thread; cached, never fatal."""
        if thread_id in self._names:
            return self._names[thread_id]
        name = ""
        try:
            if thread_type == ThreadType.GROUP:
                name = _find_name(self.fetchGroupInfo(thread_id), thread_id)
            else:
                name = _find_name(self.fetchUserInfo(thread_id), thread_id)
        except Exception:
            name = ""
        self._names[thread_id] = name
        return name

    def user_name(self, user_id):
        """Display name for a sender; websocket messages carry it, backfills do not."""
        if not user_id:
            return ""
        if user_id in self._names:
            return self._names[user_id]
        try:
            name = _find_name(self.fetchUserInfo(user_id), user_id)
        except Exception:
            name = ""
        self._names[user_id] = name
        return name

    def handle_message(self, message_object, thread_id, thread_type):
        urls = extract_images(message_object, self.args.video_thumbs)
        self._debug_log(message_object, thread_id, thread_type, urls)
        if not urls:
            return 0

        content = _as_dict(message_object.get("content"))
        params = _as_dict(content.get("params"))
        sender_id = str(message_object.get("uidFrom") or self.user_id or "")
        meta = {
            "msg_id": str(message_object.get("msgId") or ""),
            "cli_msg_id": str(message_object.get("cliMsgId") or ""),
            "ts_ms": str(message_object.get("ts") or ""),
            "sent_at": iso_from_ts(message_object.get("ts")),
            "thread_id": str(thread_id),
            "thread_type": thread_type.name if hasattr(thread_type, "name") else str(thread_type),
            "thread_name": self.thread_name(thread_id, thread_type),
            "sender_id": sender_id,
            "sender_name": message_object.get("dName") or self.user_name(sender_id),
            "msg_type": message_object.get("msgType") or "",
            "caption": content.get("title") or content.get("description") or "",
            # several images sent at once are separate messages tied together
            # by group_layout_id; nothing in one message points at its siblings
            "album_id": str(params.get("group_layout_id") or ""),
            # id_in_group is 0 for the first image, so `or ""` would lose it
            "album_index": ("" if params.get("id_in_group") is None
                            else str(params.get("id_in_group"))),
            "album_total": str(params.get("total_item_in_group") or ""),
        }

        saved = 0
        for url in urls:
            row = self.store.save(meta, url, download=not self.args.no_download)
            if row:
                saved += 1
                print(f"[{row['status']}] {meta['thread_name'] or meta['thread_id']} "
                      f"<- {meta['sender_name']}: {row['file_name'] or url}")
                self._report_album(row)
        return saved

    @staticmethod
    def _report_album(row):
        """Say so when a burst of images arrived incomplete.

        Each image of a burst is its own message, and the offline sweep only
        ever returns the newest message per thread - so a burst missed while
        disconnected cannot be completed afterwards.
        """
        total = int(row.get("album_total") or 0)
        if total < 2:
            return
        have = row.pop("_album_have", 1)
        if have >= total:
            print(f"    burst complete: {have} of {total} images")
        else:
            print(f"    burst incomplete: {have} of {total} images - the rest "
                  "were sent while not listening and cannot be fetched back")

    # -- zlapi callbacks --------------------------------------------------
    def _debug_log(self, message_object, thread_id, thread_type, urls):
        """Append one line per dispatched message, so a burst can be counted."""
        path = getattr(self.args, "debug_log", None)
        if not path:
            return
        content = _as_dict(message_object.get("content"))
        params = _as_dict(content.get("params"))
        entry = {
            "at": datetime.now().astimezone().isoformat(timespec="milliseconds"),
            "msg_id": message_object.get("msgId"),
            "cli_msg_id": message_object.get("cliMsgId"),
            "ts": message_object.get("ts"),
            "msg_type": message_object.get("msgType"),
            "thread_id": str(thread_id),
            "thread_type": getattr(thread_type, "name", str(thread_type)),
            "sender": str(message_object.get("uidFrom") or ""),
            "album_id": params.get("group_layout_id"),
            "album_index": params.get("id_in_group"),
            "album_total": params.get("total_item_in_group"),
            "image_urls": len(urls),
        }
        try:
            with open(path, "a", encoding="utf-8") as fh:
                fh.write(json.dumps(entry, ensure_ascii=False) + "\n")
        except Exception:
            pass

    def onMessage(self, mid=None, author_id=None, message=None, message_object=None,
                  thread_id=None, thread_type=ThreadType.USER):
        if message_object is None:
            return
        if self.args.threads and str(thread_id) not in self.args.threads:
            return
        try:
            self.handle_message(message_object, thread_id, thread_type)
        except Exception as exc:  # never let one bad message kill the listener
            print(f"error handling message {mid}: {exc}", file=sys.stderr)

    def onErrorCallBack(self, error):
        print(f"listener error: {error}", file=sys.stderr)


# --------------------------------------------------------------------------
# commands
# --------------------------------------------------------------------------

def _norm(text):
    """Lowercase and strip accents, so `cafe` matches `Cà Phê`."""
    text = unicodedata.normalize("NFD", str(text or "").lower())
    return "".join(ch for ch in text if not unicodedata.combining(ch))


CHAT_CACHE = ".chats_cache.json"
CHAT_CACHE_TTL = 6 * 3600


def _retry(call, attempts=3, delay=1.5):
    """Zalo throttles directory calls and answers with non-JSON; back off."""
    for attempt in range(attempts):
        try:
            return call()
        except Exception as exc:
            if attempt == attempts - 1:
                raise
            time.sleep(delay * (attempt + 1))
    return None


def _recent_threads(bot, known_ids):
    """Threads from the preload cache that the friend/group lists missed."""
    last = _as_dict(_retry(bot.getLastMsgs))
    found = []

    for bucket, kind in (("msgs", "user"), ("groupMsgs", "group"), ("pageMsgs", "user")):
        for raw in last.get(bucket) or []:
            raw = _as_dict(raw)
            tid = str(raw.get("idTo") if kind == "group"
                      else (raw.get("uidFrom") or raw.get("idTo")) or "")
            if not tid or tid in ("0", str(bot.user_id)) or tid in known_ids:
                continue
            known_ids.add(tid)
            found.append([kind, tid, raw.get("dName") or ""])

    # names are missing from this payload; look up the few that turned up
    for entry in found:
        if entry[2]:
            continue
        try:
            if entry[0] == "group":
                entry[2] = _find_name(bot.fetchGroupInfo(entry[1]), entry[1])
            else:
                entry[2] = _find_name(bot.fetchUserInfo(entry[1]), entry[1])
        except Exception:
            entry[2] = ""

    return [tuple(e) for e in found]


def list_chats(bot, cache_path=CHAT_CACHE, refresh=False, ttl=CHAT_CACHE_TTL):
    """Every group and friend as (kind, id, name), cached on disk.

    The directory costs two throttled requests, so it is cached for `ttl`
    seconds; a stale cache is still better than failing a capture run.
    """
    cached, cached_age = None, None
    if os.path.exists(cache_path):
        try:
            with open(cache_path, "r", encoding="utf-8") as fh:
                cached = [tuple(row) for row in json.load(fh)]
            cached_age = time.time() - os.path.getmtime(cache_path)
        except Exception:
            cached = None

    if cached and not refresh and cached_age is not None and cached_age < ttl:
        return cached

    chats, failed = [], []

    try:
        groups = _as_dict(_retry(bot.fetchAllGroups)).get("gridVerMap") or {}
        if groups:
            # fetchGroupInfo batches when handed a dict of ids
            info = _as_dict(_retry(lambda: bot.fetchGroupInfo(dict(groups)))).get("gridInfoMap") or {}
            for gid in groups:
                entry = _as_dict(info.get(str(gid)))
                chats.append(("group", str(gid), entry.get("name") or ""))
    except Exception as exc:
        failed.append(f"groups ({exc})")

    try:
        for friend in _retry(bot.fetchAllFriends) or []:
            friend = _as_dict(friend)
            chats.append(("user", str(friend.get("userId")),
                          friend.get("displayName") or friend.get("zaloName") or ""))
    except Exception as exc:
        failed.append(f"friends ({exc})")

    # people you chat with who are not friends never show up above
    try:
        chats.extend(_recent_threads(bot, {c[1] for c in chats}))
    except Exception as exc:
        failed.append(f"recent threads ({exc})")

    if failed and cached:
        print(f"could not refresh {', '.join(failed)} - using cached list "
              f"({int((cached_age or 0) / 60)} min old)", file=sys.stderr)
        return cached
    if failed:
        print(f"could not fetch {', '.join(failed)}", file=sys.stderr)
    if not chats:
        sys.exit("no chats available - zalo may be throttling, try again shortly")

    try:
        with open(cache_path, "w", encoding="utf-8") as fh:
            json.dump(chats, fh, ensure_ascii=False)
    except Exception:
        pass

    return chats


def normalize_phone(text):
    """Return a Vietnamese number in local 0xxxxxxxxx form, or None."""
    digits = re.sub(r"[^0-9]", "", str(text or ""))
    if not digits:
        return None
    if digits.startswith("84"):
        digits = "0" + digits[2:]
    elif not digits.startswith("0"):
        digits = "0" + digits
    return digits if 9 <= len(digits) <= 11 else None


def lookup_user(bot, text):
    """Resolve a phone number or raw user id straight from Zalo.

    This skips the chat directory entirely, so somebody you added a minute ago
    resolves without waiting for any cache.
    """
    text = str(text).strip()

    if text.isdigit() and len(text) >= 15:   # zalo user ids are ~18 digits
        try:
            name = _find_name(bot.fetchUserInfo(text), text)
            if name:
                return text, name
        except Exception:
            pass

    phone = normalize_phone(text)
    if phone:
        try:
            profile = _as_dict(bot.fetchPhoneNumber(phone))
            uid = str(profile.get("uid") or "")
            name = profile.get("display_name") or profile.get("zalo_name") or ""
            if uid:
                return uid, name
        except Exception:
            pass

    return None, None


def _remember_chat(chats, entry):
    """Add a freshly looked-up person to the cached directory."""
    if any(c[1] == entry[1] for c in chats):
        return
    chats.append(entry)
    try:
        with open(CHAT_CACHE, "w", encoding="utf-8") as fh:
            json.dump([list(c) for c in chats], fh, ensure_ascii=False)
    except Exception:
        pass


def resolve_threads(bot, names, thread_ids, refresh=False):
    """Turn --chat names into thread ids, keeping any explicit --threads ids.

    An exact id always wins. A name that matches several chats is reported
    rather than guessed at, so you never silently capture the wrong chat.
    """
    selected = list(thread_ids or [])
    if not names:
        return selected or None

    chats = list_chats(bot, refresh=refresh)
    refreshed = refresh
    by_id = {c[1]: c for c in chats}

    for name in names:
        if name in by_id:
            kind, cid, label = by_id[name]
            print(f"  {label or '(no name)'}  [{kind} {cid}]")
            selected.append(cid)
            continue

        needle = _norm(name)
        hits = [c for c in chats if needle in _norm(c[2])]
        exact = [c for c in hits if _norm(c[2]) == needle]
        if exact:
            hits = exact

        # a phone number or raw id never matches a cached *name*; asking zalo
        # costs one request, far less than rebuilding the directory
        if not hits and (normalize_phone(name) or name.isdigit()):
            uid, resolved = lookup_user(bot, name)
            if uid:
                print(f"  {resolved or '(no name)'}  [user {uid}]  (looked up directly)")
                selected.append(uid)
                _remember_chat(chats, ("user", uid, resolved or ""))
                continue

        if not hits and not refreshed:
            # a chat created minutes ago is not in the cached directory yet
            print(f'"{name}" not in the cached chat list - refreshing')
            chats = list_chats(bot, refresh=True)
            by_id = {c[1]: c for c in chats}
            refreshed = True
            if name in by_id:
                kind, cid, label = by_id[name]
                print(f"  {label or '(no name)'}  [{kind} {cid}]")
                selected.append(cid)
                continue
            hits = [c for c in chats if needle in _norm(c[2])]

        if not hits:
            uid, resolved = lookup_user(bot, name)
            if uid:
                print(f"  {resolved or '(no name)'}  [user {uid}]  (looked up directly)")
                selected.append(uid)
                continue
            sys.exit(f'no chat matches "{name}" - run the `chats` command to see '
                     "the list, or pass a phone number / user id")
        if len(hits) > 1:
            print(f'"{name}" matches {len(hits)} chats:', file=sys.stderr)
            for kind, cid, label in hits[:15]:
                print(f"  {cid}  [{kind}] {label}", file=sys.stderr)
            sys.exit("be more specific, or pass the id directly")

        kind, cid, label = hits[0]
        print(f"  {label}  [{kind} {cid}]")
        selected.append(cid)

    return selected


def load_config(path):
    with open(path, "r", encoding="utf-8") as fh:
        cfg = json.load(fh)

    cookies = cfg.get("cookies")
    # accept a browser cookie export: [{"name": ..., "value": ...}, ...]
    if isinstance(cookies, list):
        cookies = {c["name"]: c["value"] for c in cookies if c.get("name")}
    cfg["cookies"] = cookies or None
    return cfg


def build_client(cfg, store_args):
    bot = ImageCaptureBot(
        phone=cfg.get("phone") or None,
        password=cfg.get("password") or None,
        imei=cfg.get("imei"),
        cookies=cfg.get("cookies"),
        user_agent=cfg.get("user_agent") or None,
    )
    store = ImageStore(
        csv_path=store_args.csv,
        out_dir=store_args.out_dir,
        cookies=bot.getSession(),
        user_agent=cfg.get("user_agent"),
    )
    return bot.configure(store, store_args), store


def _ancestors(pid):
    """This process plus every parent, so wrappers never look like rivals."""
    chain, seen = set(), set()
    while pid and pid not in seen:
        seen.add(pid)
        chain.add(pid)
        try:
            out = subprocess.run(["ps", "-o", "ppid=", "-p", str(pid)],
                                 capture_output=True, text=True, timeout=5).stdout.strip()
        except Exception:
            break
        pid = int(out) if out.isdigit() else 0
    return chain


def other_listeners():
    """PIDs of other instances of this script already listening.

    Two listeners evict each other from zalo's single web session forever, so
    each reconnect just steals the session back and nothing stays connected.
    Only python processes count: the shell that launched us carries the same
    command text, and --reconnect re-execs in place, keeping our own pid.
    """
    try:
        out = subprocess.run(["ps", "-eo", "pid=,command="],
                             capture_output=True, text=True, timeout=10).stdout
    except Exception:
        return []

    skip = _ancestors(os.getpid())
    script = os.path.basename(__file__)
    found = []
    for line in out.splitlines():
        pid, _, command = line.strip().partition(" ")
        if not pid.isdigit() or int(pid) in skip or not command:
            continue
        if "python" not in command.split()[0].lower():
            continue
        if script in command and " listen" in command:
            found.append(int(pid))
    return found


def _start_resync(bot, args):
    """Re-run the catch-up sweep on a timer while listening.

    The websocket is the complete source, but it only covers the time the
    process is actually connected. A periodic sweep closes the gap left by a
    dropped connection without waiting for the next manual run.
    """
    def loop():
        while True:
            time.sleep(args.sync_interval)
            try:
                found = sync_pending(bot, args)
                if found:
                    print(f"re-sync picked up {found} image(s)")
            except Exception as exc:
                print(f"re-sync failed: {exc}", file=sys.stderr)

    thread = threading.Thread(target=loop, daemon=True)
    thread.start()
    return thread


def _on_session_stolen(args, store):
    """zlapi SIGTERMs its own process when the websocket goes away.

    Zalo allows one web session per account, so opening chat.zalo.me kicks the
    listener out. Turn that bare `zsh: terminated` into something actionable,
    and optionally come back. Ctrl+C reaches us the same way - zlapi converts
    KeyboardInterrupt into SIGTERM - so a user-requested stop has to be told
    apart from a stolen session, or --reconnect would resurrect it.
    """
    stopping = {"by_user": False}

    def on_sigint(signum, frame):
        stopping["by_user"] = True
        raise KeyboardInterrupt

    def on_sigterm(signum, frame):
        store.close()
        if stopping["by_user"]:
            print("\nstopped")
            os._exit(0)

        print("\nzalo closed this session - another zalo web connection opened "
              "(a chat.zalo.me tab, or a second copy of this script)")
        if not args.reconnect:
            print("close it and re-run, or use --reconnect SECONDS to retry "
                  "automatically")
            # the websocket thread would swallow a SystemExit; the csv is
            # flushed per row and the store is closed, so leaving hard is safe
            os._exit(3)
        print(f"reconnecting in {args.reconnect}s - note this will keep fighting "
              "an open browser tab for the session")
        time.sleep(args.reconnect)
        os.execv(sys.executable, [sys.executable] + sys.argv)

    signal.signal(signal.SIGINT, on_sigint)
    signal.signal(signal.SIGTERM, on_sigterm)


def cmd_listen(args, cfg):
    running = other_listeners()
    if running and not args.force:
        sys.exit(
            f"another listener is already running (pid {', '.join(map(str, running))}).\n"
            "zalo allows one web session, so two listeners evict each other and "
            "neither stays connected - which loses every image sent in between.\n"
            f"stop it first:  kill {running[0]}\n"
            "or pass --force if you really want both."
        )

    bot, store = build_client(cfg, args)
    print(f"logged in as {bot.uid()}")

    if args.chat or args.threads:
        print("capturing only from:")
        args.threads = resolve_threads(bot, args.chat, args.threads,
                                       refresh=args.refresh_chats)
    else:
        print("capturing from every chat (use --chat NAME to narrow)")

    if patch_multi_message_delivery():
        print("patched zlapi so every message in a frame is delivered")
    else:
        print("could not patch zlapi group delivery - bursts of images sent to "
              "a group may arrive incomplete", file=sys.stderr)

    print(f"csv: {args.csv}   images: {args.out_dir}")

    if args.no_sync:
        print("skipping the catch-up sync")
    else:
        pending = sync_pending(bot, args)
        print(f"synced {pending} image(s) that arrived while offline"
              if pending else "nothing pending to sync")

    if args.sync_interval:
        _start_resync(bot, args)

    print("listening, Ctrl+C to stop")
    _on_session_stolen(args, store)
    try:
        bot.listen(thread=True)
    except KeyboardInterrupt:
        print("\nstopped")
    finally:
        store.close()


def sync_pending(bot, args):
    """Capture whatever zalo still holds that is not in the CSV yet.

    Zalo keeps chat history on the client, so the only server-side source is the
    preload cache behind getLastMsgs - the newest message per thread. Dedup on
    (msg_id, url) means running this repeatedly is free.
    """
    total = 0
    buckets = [
        ("msgs", ThreadType.USER),
        ("groupMsgs", ThreadType.GROUP),
        ("pageMsgs", ThreadType.USER),
    ]
    last = _as_dict(bot.getLastMsgs())
    for bucket, thread_type in buckets:
        for raw in last.get(bucket) or []:
            raw = _as_dict(raw)
            if thread_type == ThreadType.GROUP:
                thread_id = raw.get("idTo")
            else:
                # 1-1 messages carry idTo=0; the peer is uidFrom
                thread_id = raw.get("uidFrom") or raw.get("idTo")
            if args.threads and str(thread_id) not in args.threads:
                continue
            total += bot.handle_message(raw, thread_id, thread_type)
    return total


def cmd_history(args, cfg):
    """Backfill only - the same sweep `listen` now runs at startup."""
    bot, store = build_client(cfg, args)
    if args.chat:
        print("capturing only from:")
        args.threads = resolve_threads(bot, args.chat, args.threads,
                                       refresh=args.refresh_chats)
    total = 0
    try:
        total += sync_pending(bot, args)

        for group_id in args.group or []:
            messages = _as_dict(bot.getRecentGroup(group_id)).get("groupMsgs") or []
            print(f"group {group_id}: {len(messages)} recent message(s)")
            for raw in messages:
                total += bot.handle_message(_as_dict(raw), group_id, ThreadType.GROUP)
    finally:
        store.close()
    print(f"captured {total} new image(s) -> {args.csv}")
    if not total:
        print("nothing pending - zalo serves almost no history; run `listen` to "
              "capture images as they arrive")


def cmd_whois(args, cfg):
    """Look a person up by phone number or user id, straight from Zalo."""
    bot = ImageCaptureBot(
        imei=cfg.get("imei"),
        cookies=cfg.get("cookies"),
        user_agent=cfg.get("user_agent") or None,
    )
    for target in args.who:
        uid, name = lookup_user(bot, target)
        if not uid:
            print(f"{target}\tnot found (hidden phone number, or not a zalo user)")
            continue

        username = ""
        try:
            profiles = _as_dict(bot.fetchUserInfo(uid))
            for key in ("changed_profiles", "unchanged_profiles"):
                entry = _as_dict((profiles.get(key) or {}).get(uid))
                if entry:
                    name = name or entry.get("displayName") or entry.get("zaloName") or ""
                    username = entry.get("username") or ""
                    break
        except Exception:
            pass

        print(f"{uid}\tuser\t{name}" + (f"\t@{username}" if username else ""))
        print(f'  capture it with:  --chat {uid}')


def cmd_chats(args, cfg):
    bot = ImageCaptureBot(
        imei=cfg.get("imei"),
        cookies=cfg.get("cookies"),
        user_agent=cfg.get("user_agent") or None,
    )
    chats = list_chats(bot, refresh=args.refresh_chats)

    if args.kind != "all":
        chats = [c for c in chats if c[0] == args.kind]
    if args.search:
        needle = _norm(args.search)
        chats = [c for c in chats if needle in _norm(c[2]) or args.search == c[1]]

    groups = sum(1 for c in chats if c[0] == "group")
    print(f"{len(chats)} chat(s): {groups} group, {len(chats) - groups} user\n")
    for kind, cid, name in chats:
        print(f"{cid}\t{kind}\t{name}")


def _add_common(parser, suppress=False):
    """Shared flags; subparsers suppress defaults so they cannot clobber."""
    def default(value):
        return argparse.SUPPRESS if suppress else value

    parser.add_argument("--config", default=default("config.json"), help="login config JSON")
    parser.add_argument("--csv", default=default("zalo_images.csv"), help="CSV output file")
    parser.add_argument("--out-dir", default=default("images"),
                        help="directory for downloaded images")
    parser.add_argument("--no-download", action="store_true", default=default(False),
                        help="only record URLs in the CSV, do not fetch the files")
    parser.add_argument("--video-thumbs", action="store_true", default=default(False),
                        help="also capture the poster frame of video messages")
    parser.add_argument("--refresh-chats", action="store_true", default=default(False),
                        help="re-fetch the chat directory instead of using the cache")


def main(argv=None):
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    _add_common(parser)

    shared = argparse.ArgumentParser(add_help=False)
    _add_common(shared, suppress=True)

    sub = parser.add_subparsers(dest="command", required=True)

    p_listen = sub.add_parser("listen", parents=[shared],
                              help="capture images from incoming messages")
    p_listen.add_argument("--chat", nargs="+", default=None, metavar="NAME",
                          help="only capture from these chats, by name or id "
                               "(accent- and case-insensitive)")
    p_listen.add_argument("--threads", nargs="*", default=None,
                          help="only capture from these thread ids (default: all)")
    p_listen.add_argument("--no-sync", action="store_true",
                          help="do not capture pending images before listening")
    p_listen.add_argument("--force", action="store_true",
                          help="start even if another listener is running")
    p_listen.add_argument("--debug-log", metavar="PATH", default=None,
                          help="append every dispatched message to PATH as JSONL")
    p_listen.add_argument("--sync-interval", type=int, default=120,
                          metavar="SECONDS",
                          help="re-run the catch-up sweep this often while "
                               "listening (0 disables)")
    p_listen.add_argument("--reconnect", type=int, default=0, metavar="SECONDS",
                          help="restart this many seconds after zalo drops the "
                               "session (default: exit)")
    p_listen.set_defaults(func=cmd_listen)

    p_hist = sub.add_parser("history", parents=[shared],
                            help="capture what zalo still holds server-side")
    p_hist.add_argument("--group", nargs="+", default=None,
                        help="also try getrecentv2 for these group id(s)")
    p_hist.add_argument("--chat", nargs="+", default=None, metavar="NAME",
                        help="only capture from these chats, by name or id")
    p_hist.add_argument("--threads", nargs="*", default=None,
                        help="only capture from these thread ids")
    p_hist.set_defaults(func=cmd_history)

    p_who = sub.add_parser("whois", parents=[shared],
                           help="look up a person by phone number or user id")
    p_who.add_argument("who", nargs="+", metavar="PHONE_OR_ID")
    p_who.set_defaults(func=cmd_whois, threads=None, chat=None)

    for name, helptext in (("chats", "list every chat with its id"),
                           ("groups", argparse.SUPPRESS)):  # groups: kept as an alias
        p_chats = sub.add_parser(name, parents=[shared], help=helptext)
        p_chats.add_argument("--search", default=None, metavar="TEXT",
                             help="only show chats whose name contains TEXT")
        p_chats.add_argument("--kind", choices=("all", "group", "user"),
                             default="group" if name == "groups" else "all",
                             help="which chats to list")
        p_chats.set_defaults(func=cmd_chats, threads=None, chat=None)

    args = parser.parse_args(argv)

    if not os.path.exists(args.config):
        parser.error(f"config not found: {args.config} (copy config.example.json)")
    cfg = load_config(args.config)

    args.func(args, cfg)


if __name__ == "__main__":
    main()
