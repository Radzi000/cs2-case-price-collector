import requests
import pandas as pd
from datetime import datetime
import os
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import json

def get_hashname(item, skin, wear, stat):
    if skin == "" and wear == 0:
        return item.replace(" ", "%20")
    
    item = item.replace(" ", "%20")
    skin = skin.replace(" ", "%20")
    float_map = {
        1: "%20%28Factory%20New%29",
        2: "%20%28Minimal%20Wear%29",
        3: "%20%28Field-Tested%29",
        4: "%20%28Well-Worn%29",
        5: "%20%28Battle-Scarred%29"
    }
    wear_str = float_map.get(wear, "")
    if stat == 1:
        item = "StatTrak™%20" + item
    hashname = item + "%20%7C%20" + skin + wear_str
    return hashname

def get_nameid(hashname):
    url = f"https://steamcommunity.com/market/listings/730/{hashname}"
    html = requests.get(url).text
    parts = html.split('Market_LoadOrderSpread( ')
    if len(parts) < 2:
        raise ValueError("Could not find Market_LoadOrderSpread in HTML. Item may not exist.")
    nameid_part = parts[1].split(' ')[0]
    return int(nameid_part)

def item_data(hashname):
    nameid = str(get_nameid(hashname))
    out = {}

    # Get order data (highest buy order, lowest sell order)
    order_url = f"https://steamcommunity.com/market/itemordershistogram?country=US&currency=1&language=english&two_factor=0&item_nameid={nameid}"
    order_data = requests.get(order_url).text

    # Parse highest and lowest orders
    highest_buy = (order_data.split('"highest_buy_order":"')[1]).split('"')[0]
    lowest_sell = (order_data.split('"lowest_sell_order":"')[1]).split('"')[0]
    out["Buy"] = int(highest_buy) / 100.0
    out["Sell"] = int(lowest_sell) / 100.0

    # Get volume from priceoverview
    price_url = f"https://steamcommunity.com/market/priceoverview/?appid=730&currency=1&market_hash_name={hashname}"
    price_data = requests.get(price_url).text
    volume_str = (price_data.split('volume":"')[1]).split('"')[0]
    out["Volume"] = int(volume_str.replace(",", ""))
    
    return out

def append_to_google_sheets(data, spreadsheet_name, worksheet_name):
    """
    Zapisuje wiersz do konkretnego 'worksheet_name' w arkuszu 'spreadsheet_name'.
    Jeśli dany worksheet nie istnieje, tworzy go automatycznie.
    Jeśli worksheet jest pusty, dopisuje wiersz nagłówków.
    """
    # Definiujemy scope
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive.file",
        "https://www.googleapis.com/auth/drive"
    ]

    # Autentykacja z wykorzystaniem service account
    credentials_json = os.getenv('GOOGLE_CREDENTIALS_JSON')
    creds_dict = json.loads(credentials_json)
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    client = gspread.authorize(creds)

    # Otwieramy spreadsheet
    spreadsheet = client.open(spreadsheet_name)
    
    # Sprawdzamy, czy worksheet o danej nazwie istnieje
    try:
        sheet = spreadsheet.worksheet(worksheet_name)
    except gspread.exceptions.WorksheetNotFound:
        # Jeśli nie istnieje, tworzymy
        sheet = spreadsheet.add_worksheet(title=worksheet_name, rows=1000, cols=10)

    # Sprawdzamy, czy worksheet jest pusty (nie zawiera żadnych danych)
    existing_data = sheet.get_all_values()
    if len(existing_data) == 0:
        # Dodajemy nagłówki na górze
        sheet.append_row(["Time", "Buy", "Sell", "Volume"])

    # Przygotowujemy wiersz do dodania
    row = [
        data['Time'].strftime("%Y-%m-%d %H:%M:%S"),
        data['Buy'],
        data['Sell'],
        data['Volume']
    ]

    # Dodajemy wiersz poniżej nagłówków
    sheet.append_row(row)

def main():
    # Lista skrzynek, dla których chcesz pobrać dane
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
        "Glove Case"
    ]

    # Nazwa głównego arkusza w Google (Spreadsheet)
    spreadsheet_name = "CS2 Case Data"

    # Pobieramy czas "na teraz"
    current_time = datetime.utcnow()

    # Dla każdej skrzynki w liście zbieramy dane i zapisujemy do osobnej zakładki
    for item_name in cases:
        try:
            # Generujemy hash_name (skin, wear, stat ustawione na 0/"" - możesz dopasować)
            hash_name = get_hashname(item_name, "", 0, 0)
            data = item_data(hash_name)

            # Dokładamy do słownika datę/czas — ten sam dla każdej skrzynki
            data["Time"] = current_time

            # Zapis do worksheet o nazwie np. "Prisma Case", "Snakebite Case" itd.
            append_to_google_sheets(data, spreadsheet_name, item_name)

            print(f"Zaktualizowano dane dla: {item_name}")

        except Exception as e:
            print(f"Wystąpił błąd dla {item_name}: {e}")
            # raise e

    print("Wszystkie dostępne dane zaktualizowane pomyślnie.")

if __name__ == "__main__":
    main()
