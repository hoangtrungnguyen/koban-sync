# Zalo image capture → CSV

Capture every image sent in your Zalo chats with the [`zlapi`](https://pypi.org/project/zlapi/)
library, download the files, and log one CSV row per image.

> Unofficial library — it drives your own Zalo account through the web API.
> Use it on chats you are part of, and expect Zalo to change things under you.

## Install

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
```

## Credentials

`zlapi` only supports cookie login (phone + password raises
`LoginMethodNotSupport`), and the cookies must be paired with the **IMEI** —
the device id Zalo Web generated for that browser profile.

### Cookies

Open <https://chat.zalo.me> logged in → DevTools → **Application → Cookies →
`https://chat.zalo.me`**. Copy every cookie (`zpsid`, `zpw_sek`, `_zlang`, …).
The important ones are HttpOnly, so `document.cookie` will NOT show them.

### IMEI

Zalo sends it in the clear on exactly one request, which is the surest place to
read it:

1. DevTools → **Network**, filter `getLoginInfo`.
2. Reload chat.zalo.me.
3. Click the `getLoginInfo` request → **Payload / Query String Parameters** →
   copy the `imei` value.

If that request does not appear (already cached), filter on `imei` instead and
check the query string of any `*.chat.zalo.me/api/...` call, or read it from
storage: DevTools → **Application → Local Storage → `https://chat.zalo.me`**,
look for the uuid-shaped value.

### Verify before running

```bash
cp config.example.json config.json   # paste imei + cookies
.venv/bin/python check_login.py
```

`OK - imei accepted` plus your uid means the pair works. `REJECTED` means the
imei belongs to a different session or the cookies expired.

Not sure which stored value is the IMEI? Dump local storage in the chat.zalo.me
console:

```js
copy(JSON.stringify(Object.fromEntries(Object.entries(localStorage))))
```

Save it as `ls.json` and let the script find the right one — it tries each
uuid-shaped value against Zalo and prints the one that authenticates:

```bash
.venv/bin/python check_login.py --localstorage ls.json
```

Cookies and IMEI are a matched pair: re-copy both if you log out, and keep
`config.json` out of version control (already in `.gitignore`).

## Use

List your chats (groups, friends, and anyone you have a recent thread with):

```bash
.venv/bin/python zalo_image_capture.py chats --search flutter
```

```
5738330348692482124	group	Flutter Team
8660443554950319582	group	Flutter 2
7969413614792520311	user	Park Flutter
```

`listen` first syncs anything pending — images that arrived while it was not
running — and then keeps the websocket open:

```
  Sơn Kim  [user 6029818795262654528]  (looked up directly)
[ok] Sơn Kim <- Sơn Kim: 1789295099413_6029818795262654528_8258780518815.jpg
synced 1 image(s) that arrived while offline
listening, Ctrl+C to stop
```

Rows are deduplicated, so that sync costs nothing when there is nothing new.
`--no-sync` skips it; `history` runs the same sweep without listening.

Capture from the chats you pick — by name or by id:

```bash
.venv/bin/python zalo_image_capture.py listen --chat "Flutter Team"
```

```bash
.venv/bin/python zalo_image_capture.py listen --chat "tan tai" 5738330348692482124
```

Matching ignores case and accents, so `tan tai` finds `Tấn Tài`. A name that hits
several chats is never guessed at — the matches are printed so you can pick:

```
"flutter" matches 3 chats:
  5738330348692482124  [group] Flutter Team
  8660443554950319582  [group] Flutter 2
  7969413614792520311  [user] Park Flutter
be more specific, or pass the id directly
```

With no `--chat`, every chat is captured. Backfill what the server still holds:

```bash
.venv/bin/python zalo_image_capture.py history --chat "Flutter Team"
```

| flag | meaning |
| --- | --- |
| `--chat NAME …` | capture only these chats, by name or id |
| `--threads ID …` | same, ids only |
| `--csv PATH` | CSV output (default `zalo_images.csv`) |
| `--out-dir DIR` | where images land (default `images/`) |
| `--no-download` | record URLs in the CSV, skip fetching the files |
| `--video-thumbs` | also capture the poster frame of video messages |
| `--no-sync` | `listen` only: skip the catch-up sync and go straight to listening |
| `--sync-interval N` | `listen` only: re-run the sweep every N seconds (default 120, 0 off) |
| `--refresh-chats` | re-fetch the chat directory instead of using the cache |
| `--force` | start even though another listener is running (normally refused) |
| `--debug-log PATH` | append every dispatched message to PATH as JSONL |
| `--reconnect N` | `listen` only: restart N seconds after Zalo drops the session |

Flags work before or after the sub-command.

The chat directory is cached in `.chats_cache.json` for 6 hours, because Zalo
throttles those calls and starts answering with non-JSON. `--refresh-chats`
forces a re-fetch; a new chat that is not in the cache needs it.

### Somebody you just added

The directory cache is 6 hours old at worst, and `fetchAllFriends` only returns
accepted friends — so a contact added a minute ago may not be listed. Three ways
around it, cheapest first:

```bash
.venv/bin/python zalo_image_capture.py whois 0912345678
```

`whois` takes a phone number (`0912…`, `84912…`, `+84 912…`) or a user id and
asks Zalo directly — no cache, no friendship required:

```
4684907536476307690	user	Tấn Tài	@t_m7da0f6zm0
  capture it with:  --chat 4684907536476307690
```

`--chat` does the same on its own: a name that is not in the cache triggers one
refresh, and a phone number or id that still does not match is looked up live.

```bash
.venv/bin/python zalo_image_capture.py listen --chat 0912345678
```

`--refresh-chats` rebuilds the whole directory if you would rather match by name.

Nothing is needed for the CSV itself: `sender_name` is resolved per message at
capture time, so a brand-new contact's name lands in the file with no setup.

### Only one listener at a time

Two listeners evict each other from the single web session forever - each
reconnect steals it back - so nothing stays connected and every image sent in
between is lost.  refuses to start when another one is running:



### Only one listener at a time

Two listeners evict each other from the single web session forever - each
reconnect steals it back - so neither stays connected and every image sent in
between is lost. `listen` now refuses to start when another one is running:

```
another listener is already running (pid 87360).
stop it first:  kill 87360
```

Pass `--force` only if you really mean it.

### Another Zalo PC/Web client kicks the listener

`zlapi` logs in as `zpw_type=30` — the same client class as the Zalo **PC app**
and Zalo Web. Zalo permits one, so if any other one is attached the server
closes our websocket about 170 ms after it opens:

```
18:06:45.911  run_forever(reconnect=0)
18:06:46.079  ws on_open
              WARN Another connection is opened, closing this one
```

The listener is then offline for everything that follows, which is why a burst
of images comes back as one. The login response says whether this is happening,
and `check_login.py` reports it:

```
WARNING: another Zalo PC/Web client is attached to this account.
```

Fix it outside this tool: quit Zalo on your other computer, or log that device
out from the phone app (Settings > Account and Security > Logged-in devices).
The phone app itself is a different client class and does not conflict.

### Only one Zalo Web session at a time

Zalo permits a single web connection per account. When another one opens,
`zlapi` terminates its own process — so **close every chat.zalo.me tab before
running `listen`**, and expect the listener to die the moment you open one.

The listener catches that and says so, exiting with code 3:

```
zalo closed this session - another zalo web connection opened (a chat.zalo.me
tab, or a second copy of this script)
close it and re-run, or use --reconnect SECONDS to retry automatically
```

`--reconnect 30` restarts it automatically. Use it for transient drops only: it
will ping-pong with a browser tab forever, each side kicking the other.

## What gets captured

| message type | captured |
| --- | --- |
| `chat.photo` (incl. multi-photo albums) | every photo, highest quality link available |
| `chat.gif` | yes |
| `share.file` with an image extension | yes |
| `chat.video.msg` | poster frame only, with `--video-thumbs` |
| everything else | ignored |

## CSV columns

`captured_at, msg_id, cli_msg_id, sent_at, ts_ms, thread_id, thread_type,
thread_name, sender_id, sender_name, msg_type, caption, url, file_name,
saved_path, width, height, file_size, sha256, status`

`status` is `ok`, `url_only` (with `--no-download`), or `download_error: …`.

Rows are de-duplicated on `(msg_id, url)` and the CSV is appended + flushed per
image, so the script can be stopped and restarted at any time without losing or
duplicating work. Rows that failed to download are retried on the next run.
`width`/`height` are filled in only when Pillow is installed.

## Several images at once

Two separate things go wrong when somebody sends a burst of images, and only one
of them is fixable.

**Fixed — group bursts over the websocket.** `zlapi` 1.0.3 builds its message
object *after* the loop over `groupMsgs`, so a frame carrying several group
messages delivers only its last one. `listen` rebinds a corrected `_listen` at
startup and says so:

```
patched zlapi so every message in a frame is delivered
```

The patch matches the known 1.0.3 source; a later zlapi that rewrites that code
simply will not match, and the library is left alone (you get a warning). One-to-one
chats were never affected — that branch already loops correctly.

**How Zalo actually sends a burst.** Not one message with several photos —
*N separate messages*, one photo each, linked only by `group_layout_id` in
`content.params`:

```json
{"total_item_in_group": 2, "id_in_group": 0, "group_layout_id": 1789296152642}
```

Those three land in the CSV as `album_id`, `album_index`, `album_total`, and the
listener says whether the burst arrived whole:

```
[ok] Sơn Kim <- Sơn Kim: 1789296152773_..._8258835988052.jpg
    burst incomplete: 1 of 2 images - the rest were sent while not listening
    and cannot be fetched back
```

**Not fixable — a burst sent while nothing was listening.** The server keeps
exactly one message per thread, the newest. Passing `threadIdLocalMsgId` as a
cursor (`0`, `1`, or a real older msgId) changes nothing — verified. So five
images sent while the listener was down recover as one, and the rest live only
in your Zalo client.

`listen` therefore syncs on startup *and* every `--sync-interval` seconds
(default 120), which narrows the window a dropped connection can leave, but
uptime is what actually captures everything.

## Limits — read this before expecting a backfill

**Zalo keeps chat history on the client, not on the server.** A fresh session
cannot ask for old messages: `getrecentv2` (what `zlapi.getRecentGroup` calls)
answers `error: 0, hasMore: 0` with an empty list for every group and every
cursor value. Verified against this account across multiple groups.

So:

- `listen` is the real capture path. Leave it running; it gets every image from
  the moment it starts.
- `history` scrapes the only server-side cache there is — `getLastMsgs`, the
  newest message per thread (1-1, group, and OA) — so it typically yields a
  handful of images, not an archive. Run it once before starting `listen` to
  catch what is sitting in the preload cache.
- `history --group <id>` additionally tries `getrecentv2` for those groups. It
  costs nothing and will start working if Zalo ever serves history again.
- Zalo image links expire. Download (the default) rather than storing only URLs.

Photos come back at full resolution — `content.href` is the original upload
(verified: 1920x1080 and 960x1280 JPEGs), not a thumbnail.
