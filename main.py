import requests
from datetime import datetime
import os
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import json
import time

def get_hashname(item, skin, wear, stat):
    """
    Tworzy nazwę hash dla danego przedmiotu na podstawie jego nazwy, skina, stanu zużycia i statystyk (StatTrak™).
    """
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
    """
    Pobiera `nameid` dla danego `hashname` poprzez analizę HTML strony Steam Community Market.
    """
    url = f"https://steamcommunity.com/market/listings/730/{hashname}"
    response = requests.get(url)
    if response.status_code != 200:
        raise ValueError(f"Nie można połączyć się z Steam Market dla {hashname}. Status kod: {response.status_code}")
    html = response.text
    parts = html.split('Market_LoadOrderSpread( ')
    if len(parts) < 2:
        raise ValueError("Nie znaleziono Market_LoadOrderSpread w HTML. Przedmiot może nie istnieć.")
    nameid_part = parts[1].split(' ')[0]
    return int(nameid_part)

def item_data(hashname):
    """
    Pobiera dane dotyczące najwyższej ceny kupna, najniższej ceny sprzedaży oraz wolumenu sprzedaży dla danego przedmiotu.
    """
    nameid = str(get_nameid(hashname))
    out = {}

    # Pobierz dane zamówień (najwyższa cena kupna, najniższa cena sprzedaży)
    order_url = f"https://steamcommunity.com/market/itemordershistogram?country=US&currency=1&language=english&two_factor=0&item_nameid={nameid}"
    order_data = requests.get(order_url).text

    try:
        # Parsowanie najwyższej ceny kupna
        highest_buy = (order_data.split('"highest_buy_order":"')[1]).split('"')[0]
        # Parsowanie najniższej ceny sprzedaży
        lowest_sell = (order_data.split('"lowest_sell_order":"')[1]).split('"')[0]
        out["Buy"] = int(highest_buy) / 100.0
        out["Sell"] = int(lowest_sell) / 100.0
    except (IndexError, ValueError):
        out["Buy"] = "N/A"
        out["Sell"] = "N/A"

    try:
        # Pobierz wolumen z priceoverview
        price_url = f"https://steamcommunity.com/market/priceoverview/?appid=730&currency=1&market_hash_name={hashname}"
        price_data = requests.get(price_url).text
        volume_str = (price_data.split('"volume":"')[1]).split('"')[0]
        out["Volume"] = int(volume_str.replace(",", ""))
    except (IndexError, ValueError):
        out["Volume"] = "N/A"
    
    return out

def append_to_google_sheets(data, sheet_name='CS2 Case Data', worksheet_name='Arkusz2'):
    """
    Dodaje zebrane dane do Google Sheets w określonym arkuszu.
    """
    # Definicja zakresu
    scope = ["https://spreadsheets.google.com/feeds",
             "https://www.googleapis.com/auth/spreadsheets",
             "https://www.googleapis.com/auth/drive.file",
             "https://www.googleapis.com/auth/drive"]

    # Autoryzacja za pomocą konta serwisowego
    credentials_json = os.getenv('GOOGLE_CREDENTIALS_JSON')
    creds_dict = json.loads(credentials_json)
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    client = gspread.authorize(creds)

    # Otwórz arkusz kalkulacyjny
    spreadsheet = client.open(sheet_name)

    try:
        sheet = spreadsheet.worksheet(worksheet_name)
    except gspread.exceptions.WorksheetNotFound:
        # Jeśli arkusz nie istnieje, utwórz go
        sheet = spreadsheet.add_worksheet(title=worksheet_name, rows="1000", cols="100")

    # Pobierz istniejące nagłówki
    headers = sheet.row_values(1)
    new_headers = False

    # Jeśli arkusz jest pusty, dodaj nagłówki
    if not headers:
        headers = ["Time"]
        for key in data.keys():
            if key != "Time":
                headers.append(key)
        sheet.append_row(headers)
    else:
        # Sprawdź, czy wszystkie klucze istnieją w nagłówkach
        for key in data.keys():
            if key not in headers:
                headers.append(key)
                new_headers = True

        if new_headers:
            # Aktualizuj nagłówki
            sheet.delete_row(1)  # Usuń stare nagłówki
            sheet.insert_row(headers, 1)  # Wstaw nowe nagłówki

    # Przygotuj wiersz do dodania
    row = []
    for header in headers:
        if header in data:
            row.append(data[header])
        else:
            row.append("")  # Puste pole, jeśli brak danych

    # Dodaj wiersz
    sheet.append_row(row)

def main():
    try:
        # Lista przedmiotów do monitorowania
        items = [
            {"item": "Prisma Case", "skin": "", "wear": 0, "stat": 0},
            {"item": "M4A4 The Emperor", "skin": "The Emperor", "wear": 1, "stat": 1},
            {"item": "Five-SeveN Angry Mob", "skin": "Angry Mob", "wear": 2, "stat": 1},
            {"item": "XM1014 Incinegator", "skin": "Incinegator", "wear": 2, "stat": 1},
            {"item": "AUG Momentum", "skin": "Momentum", "wear": 3, "stat": 0},
            {"item": "R8 Revolver Skull Crusher", "skin": "Skull Crusher", "wear": 2, "stat": 1},
            {"item": "AWP Atheris", "skin": "Atheris", "wear": 1, "stat": 1},
            {"item": "Desert Eagle Light Rail", "skin": "Light Rail", "wear": 2, "stat": 0},
            {"item": "Tec-9 Bamboozle", "skin": "Bamboozle", "wear": 2, "stat": 1},
            {"item": "UMP-45 Moonrise", "skin": "Moonrise", "wear": 3, "stat": 1},
            {"item": "MP5-SD Gauss", "skin": "Gauss", "wear": 2, "stat": 1},
            {"item": "AK-47 Uncharted", "skin": "Uncharted", "wear": 3, "stat": 1},
            {"item": "MAC-10 Whitefish", "skin": "Whitefish", "wear": 2, "stat": 1},
            {"item": "Galil AR Akoben", "skin": "Akoben", "wear": 3, "stat": 1},
            {"item": "P250 Verdigris", "skin": "Verdigris", "wear": 2, "stat": 1},
            {"item": "FAMAS Crypsis", "skin": "Crypsis", "wear": 2, "stat": 1},
            {"item": "P90 Off World", "skin": "Off World", "wear": 2, "stat": 1},
            {"item": "MP7 Mischief", "skin": "Mischief", "wear": 2, "stat": 1}
            # Możesz dodać więcej przedmiotów tutaj
        ]

        all_data = {"Time": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")}

        for itm in items:
            hash_name = get_hashname(itm["item"], itm["skin"], itm["wear"], itm["stat"])
            try:
                current_data = item_data(hash_name)
                # Tworzymy unikalne klucze dla każdego przedmiotu
                all_data[f"{itm['item']} Buy"] = current_data["Buy"] if current_data["Buy"] is not None else "N/A"
                all_data[f"{itm['item']} Sell"] = current_data["Sell"] if current_data["Sell"] is not None else "N/A"
                all_data[f"{itm['item']} Volume"] = current_data["Volume"] if current_data["Volume"] is not None else "N/A"
            except Exception as e:
                all_data[f"{itm['item']} Buy"] = "Error"
                all_data[f"{itm['item']} Sell"] = "Error"
                all_data[f"{itm['item']} Volume"] = "Error"
                print(f"Error fetching data for {itm['item']}: {e}")
                continue  # Kontynuuj z kolejnymi przedmiotami

            # Dodaj opóźnienie, aby uniknąć zbyt szybkiego wysyłania zapytań
            time.sleep(1)

        # Append data to Google Sheets (Arkusz2)
        append_to_google_sheets(all_data, worksheet_name='Arkusz2')

        print('Dane zostały pomyślnie zebrane i dodane do Google Sheets.')
    except Exception as e:
        print(f'Wystąpił błąd: {e}')
        raise e

if __name__ == "__main__":
    main()
