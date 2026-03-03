import os
import re
import json
import time
from datetime import datetime, timezone, timedelta

import requests
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# -----------------------------
# SETTINGS
# -----------------------------
BUCKET_MINUTES = 15
SLEEP_BETWEEN_ITEMS_SEC = 1.0  # ogranicza rate-limit
HTTP_TIMEOUT = 20

STEAM_HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/123.0 Safari/537.36",
    "Accept": "text/html,application/json;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "keep-alive",
}

SCOPE = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive.file",
    "https://www.googleapis.com/auth/drive",
]

FLOAT_MAP = {
    1: "%20%28Factory%20New%29",
    2: "%20%28Minimal%20Wear%29",
    3: "%20%28Field-Tested%29",
    4: "%20%28Well-Worn%29",
    5: "%20%28Battle-Scarred%29",
}


# -----------------------------
# HELPERS
# -----------------------------
def floor_to_bucket_utc(dt: datetime, bucket_minutes: int) -> datetime:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    dt = dt.astimezone(timezone.utc)
    minute = (dt.minute // bucket_minutes) * bucket_minutes
    return dt.replace(minute=minute, second=0, microsecond=0)

def get_hashname(item: str, skin: str, wear: int, stat: int) -> str:
    if skin == "" and wear == 0:
        return item.replace(" ", "%20")

    item_enc = item.replace(" ", "%20")
    skin_enc = skin.replace(" ", "%20")
    wear_str = FLOAT_MAP.get(wear, "")
    if stat == 1:
        item_enc = "StatTrak™%20" + item_enc
    return f"{item_enc}%20%7C%20{skin_enc}{wear_str}"

def requests_session_with_retries() -> requests.Session:
    s = requests.Session()
    # Prosty retry/backoff bez dodatkowych bibliotek
    s.headers.update(STEAM_HEADERS)
    return s

def http_get_text(session: requests.Session, url: str, max_tries: int = 4) -> str:
    last_err = None
    for i in range(max_tries):
        try:
            r = session.get(url, timeout=HTTP_TIMEOUT)
            # Steam czasem daje 429/403
            if r.status_code in (429, 403, 502, 503, 504):
                raise RuntimeError(f"HTTP {r.status_code} for {url}")
            r.raise_for_status()
            return r.text
        except Exception as e:
            last_err = e
            time.sleep(1.5 * (2 ** i))
    raise RuntimeError(f"GET failed after retries: {url}. Last error: {last_err}")

def http_get_json(session: requests.Session, url: str, max_tries: int = 4) -> dict:
    last_err = None
    for i in range(max_tries):
        try:
            r = session.get(url, timeout=HTTP_TIMEOUT)
            if r.status_code in (429, 403, 502, 503, 504):
                raise RuntimeError(f"HTTP {r.status_code} for {url}")
            r.raise_for_status()
            return r.json()
        except Exception as e:
            last_err = e
            time.sleep(1.5 * (2 ** i))
    raise RuntimeError(f"GET JSON failed after retries: {url}. Last error: {last_err}")

def get_nameid(session: requests.Session, hashname: str) -> int:
    url = f"https://steamcommunity.com/market/listings/730/{hashname}"
    html = http_get_text(session, url)

    # Najczęściej działa ten wzorzec:
    m = re.search(r"Market_LoadOrderSpread\(\s*(\d+)\s*\)", html)
    if m:
        return int(m.group(1))

    # Fallback: czasem w innych fragmentach
    m2 = re.search(r'"item_nameid"\s*:\s*"(\d+)"', html)
    if m2:
        return int(m2.group(1))

    # Jeśli Steam zwróci age-check / error page, tu wylądujesz
    raise ValueError("Could not find nameid in listing HTML (blocked/redirect/item missing).")

def item_data(session: requests.Session, hashname: str) -> dict:
    nameid = str(get_nameid(session, hashname))
    out = {}

    # Order histogram
    order_url = (
        "https://steamcommunity.com/market/itemordershistogram"
        f"?country=US&currency=1&language=english&two_factor=0&item_nameid={nameid}"
    )
    order_json = http_get_json(session, order_url)
    # Uwaga: wartości bywają stringami liczb w centach
    highest_buy = int(order_json.get("highest_buy_order", "0"))
    lowest_sell = int(order_json.get("lowest_sell_order", "0"))
    out["Buy"] = highest_buy / 100.0 if highest_buy > 0 else None
    out["Sell"] = lowest_sell / 100.0 if lowest_sell > 0 else None

    # Price overview (volume bywa None / brak)
    price_url = (
        "https://steamcommunity.com/market/priceoverview/"
        f"?appid=730&currency=1&market_hash_name={hashname}"
    )
    price_json = http_get_json(session, price_url)
    vol = price_json.get("volume", None)
    if isinstance(vol, str):
        vol = int(vol.replace(",", "")) if vol.strip() else None
    out["Volume"] = vol

    return out

def get_gspread_client():
    credentials_json = os.getenv("GOOGLE_CREDENTIALS_JSON")
    if not credentials_json:
        raise RuntimeError("Missing env var GOOGLE_CREDENTIALS_JSON (GitHub Actions Secret?).")

    try:
        creds_dict = json.loads(credentials_json)
    except Exception as e:
        raise RuntimeError(f"GOOGLE_CREDENTIALS_JSON is not valid JSON: {e}")

    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, SCOPE)
    return gspread.authorize(creds)

def append_to_google_sheets(
    client,
    data: dict,
    spreadsheet_name: str,
    worksheet_name: str,
    dedupe_on_time: bool = True
):
    spreadsheet = client.open(spreadsheet_name)

    try:
        sheet = spreadsheet.worksheet(worksheet_name)
    except gspread.exceptions.WorksheetNotFound:
        sheet = spreadsheet.add_worksheet(title=worksheet_name, rows=5000, cols=10)

    # Header if empty
    existing = sheet.get_all_values()
    if len(existing) == 0:
        sheet.append_row(["Time", "Buy", "Sell", "Volume"])

    time_str = data["Time"].strftime("%Y-%m-%d %H:%M:%S")

    # Optional dedupe: jeśli ostatni zapis ma ten sam Time, to nie dopisuj drugi raz
    if dedupe_on_time and len(existing) >= 2:
        last_row = existing[-1]
        if len(last_row) > 0 and last_row[0] == time_str:
            return  # skip duplicate bucket

    row = [
        time_str,
        "" if data.get("Buy") is None else data["Buy"],
        "" if data.get("Sell") is None else data["Sell"],
        "" if data.get("Volume") is None else data["Volume"],
    ]
    sheet.append_row(row)


# -----------------------------
# MAIN
# -----------------------------
def main():
    cases = [
        "Prisma Case",
        "Snakebite Case",
        "Prisma 2 Case",
        "Clutch Case",
        "Dreams & Nightmares Case",
        "Recoil Case",
        "Fracture Case",
        "Revolution Case",
        "Anubis Collection Package",
        "Danger Zone Case",
        "Horizon Case",
        "CS20 Case",
        "Spectrum 2 Case",
        "Spectrum Case",
        "Falchion Case",
        "Gamma Case",
        "Gamma 2 Case",
        "Chroma 3 Case",
        "Glove Case",
    ]

    spreadsheet_name = "CS2 Case Data"

    # stały bucket co 15 min (UTC)
    now_utc = datetime.now(timezone.utc)
    bucket_time = floor_to_bucket_utc(now_utc, BUCKET_MINUTES)

    session = requests_session_with_retries()
    gclient = get_gspread_client()

    for item_name in cases:
        try:
            hash_name = get_hashname(item_name, "", 0, 0)
            data = item_data(session, hash_name)
            data["Time"] = bucket_time

            append_to_google_sheets(
                gclient,
                data,
                spreadsheet_name=spreadsheet_name,
                worksheet_name=item_name,
                dedupe_on_time=True,
            )

            print(f"OK: {item_name} @ {bucket_time.isoformat()}")
        except Exception as e:
            print(f"ERROR: {item_name}: {e}")

        time.sleep(SLEEP_BETWEEN_ITEMS_SEC)

    print("DONE")

if __name__ == "__main__":
    main()
