"""Verify (or discover) the IMEI + cookies pair zlapi needs.

zlapi logs in by calling Zalo's getLoginInfo endpoint with your session cookies
and the IMEI (device id) those cookies were issued for. This script calls the
same endpoint directly, so you get a yes/no answer in one second instead of a
ZaloLoginError from deep inside the library.

    python check_login.py                       # test the imei in config.json
    python check_login.py --imei <candidate>    # test one candidate imei
    python check_login.py --localstorage ls.json  # try every value in a dump

`ls.json` is whatever this prints in the chat.zalo.me DevTools console:

    copy(JSON.stringify(Object.fromEntries(Object.entries(localStorage))))
"""

import argparse
import json
import os
import re
import sys
import time

import requests

LOGIN_INFO = "https://wpa.chat.zalo.me/api/login/getLoginInfo"
UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")

# zalo web device ids are uuid-shaped, sometimes with an extra suffix
UUIDISH = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}", re.I)


def try_imei(imei, cookies, timeout=15):
    """Return (ok, detail). ok is True when Zalo accepts this imei+cookies."""
    url = (f"{LOGIN_INFO}?imei={imei}&type=30&client_version=645"
           f"&computer_name=Web&ts={int(time.time() * 1000)}")
    try:
        resp = requests.get(url, headers={"User-Agent": UA,
                                          "Referer": "https://chat.zalo.me/"},
                            cookies=cookies, timeout=timeout)
        data = resp.json()
    except Exception as exc:
        return False, f"request failed: {exc}"

    payload = data.get("data")
    if data.get("error_code") == 0 and isinstance(payload, dict) and payload.get("zpw_enk"):
        return True, {
            "uid": payload.get("uid"),
            "phone_number": payload.get("phone_number"),
            "has_secret_key": bool(payload.get("zpw_enk")),
            "websockets": len(payload.get("zpw_ws") or []),
            # zalo allows one client of this type; another one kicks the
            # listener off its websocket within a fraction of a second
            "other_pc_client": bool(payload.get("haspcclient")),
        }
    return False, f"error_code={data.get('error_code')} {data.get('error_message')}"


def candidates_from_localstorage(path):
    """Pull uuid-shaped strings out of a localStorage dump, most likely first."""
    with open(path, "r", encoding="utf-8") as fh:
        raw = fh.read()

    try:
        store = json.loads(raw)
    except ValueError:
        store = {}

    found = []

    def add(key, value):
        if not isinstance(value, str):
            return
        match = UUIDISH.search(value)
        if not match:
            return
        value = value.strip()
        # a device id may carry a suffix; a JSON blob that merely contains a
        # uuid should contribute only the uuid itself
        candidate = value if (match.start() == 0 and len(value) <= 80) else match.group(0)
        if candidate not in [f[1] for f in found]:
            found.append((key, candidate))

    def walk(key, value):
        if isinstance(value, str):
            add(key, value)
            try:
                walk(key, json.loads(value))
            except (ValueError, TypeError):
                pass
        elif isinstance(value, dict):
            for k, v in value.items():
                walk(f"{key}.{k}" if key else k, v)
        elif isinstance(value, list):
            for item in value:
                walk(key, item)

    for key, value in (store.items() if isinstance(store, dict) else []):
        walk(key, value)

    # last resort: any uuid anywhere in the raw text
    for match in UUIDISH.findall(raw):
        add("(raw)", match)

    # keys naming the thing outright are the likeliest
    def score(item):
        key = item[0].lower()
        for rank, word in enumerate(("imei", "uuid", "device", "did")):
            if word in key:
                return (rank, key)
        return (9, key)

    return sorted(found, key=score)


def load_cookies(config_path):
    if not os.path.exists(config_path):
        full = os.path.abspath(config_path)
        sys.exit(
            f"config not found: {full}\n"
            "create it from the template, then paste your imei + cookies:\n"
            "    cp config.example.json config.json"
        )
    try:
        with open(config_path, "r", encoding="utf-8") as fh:
            cfg = json.load(fh)
    except ValueError as exc:
        sys.exit(f"{config_path} is not valid JSON: {exc}")
    cookies = cfg.get("cookies")
    if isinstance(cookies, list):
        cookies = {c["name"]: c["value"] for c in cookies if c.get("name")}
    if not cookies:
        sys.exit(f'no cookies in {config_path} - fill the "cookies" object '
                 "with the values from DevTools > Application > Cookies")
    placeholder = [k for k, v in cookies.items() if str(v).strip(".") == ""]
    if placeholder:
        sys.exit(f"{config_path} still has placeholder cookie value(s): "
                 f"{', '.join(placeholder)}\n"
                 "copy the real values from DevTools > Application > Cookies > "
                 "https://chat.zalo.me")
    if str(cfg.get("imei", "")).startswith("PASTE"):
        sys.exit(f"{config_path} still has the placeholder imei - see the "
                 "README section 'IMEI'")
    return cfg, cookies


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--config", default="config.json")
    parser.add_argument("--imei", help="test this imei instead of the one in the config")
    parser.add_argument("--localstorage", help="localStorage dump to scan for candidates")
    args = parser.parse_args(argv)

    cfg, cookies = load_cookies(args.config)
    print(f"cookies: {', '.join(sorted(cookies))}")

    if args.localstorage:
        candidates = candidates_from_localstorage(args.localstorage)
        if not candidates:
            sys.exit("no uuid-shaped values found in that dump")
        print(f"{len(candidates)} candidate(s) to try\n")
        for key, imei in candidates:
            ok, detail = try_imei(imei, cookies)
            print(f"{'MATCH  ' if ok else 'no     '} {imei}   (localStorage key: {key})")
            if ok:
                print(f"\nworking imei: {imei}")
                print(f"account: uid={detail['uid']} phone={detail['phone_number']}")
                print('\nput it in config.json:  "imei": "%s"' % imei)
                return 0
        print("\nnone worked - cookies may be stale, re-copy them and try again")
        return 1

    imei = args.imei or cfg.get("imei")
    if not imei:
        sys.exit("no imei given (use --imei or put one in the config)")

    ok, detail = try_imei(imei, cookies)
    if ok:
        print(f"OK - imei accepted\nuid={detail['uid']} phone={detail['phone_number']} "
              f"websockets={detail['websockets']}")
        if detail.get("other_pc_client"):
            print("\nWARNING: another Zalo PC/Web client is attached to this account.")
            print("zalo allows one client of this type, so it will kick `listen` off")
            print("its websocket immediately and images sent meanwhile are lost.")
            print("quit Zalo on your other computer, or log that device out from")
            print("the phone app (Settings > Account and Security > Logged-in devices).")
        else:
            print("no other PC/Web client attached - `listen` can hold the session")
        return 0
    print(f"REJECTED - {detail}")
    print("either the imei is not the one this browser session was issued, "
          "or the cookies expired")
    return 1


if __name__ == "__main__":
    sys.exit(main())
