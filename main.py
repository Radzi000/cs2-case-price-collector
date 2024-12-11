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

def append_to_google_sheets(data, sheet_name='CS2 Case Data'):
    # Define the scope
    scope = ["https://spreadsheets.google.com/feeds",
             "https://www.googleapis.com/auth/spreadsheets",
             "https://www.googleapis.com/auth/drive.file",
             "https://www.googleapis.com/auth/drive"]

    # Authenticate using the service account
    credentials_json = os.getenv('GOOGLE_CREDENTIALS_JSON')
    creds_dict = json.loads(credentials_json)
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    client = gspread.authorize(creds)

    # Open the spreadsheet
    sheet = client.open(sheet_name).sheet1

    # Prepare the row to append
    row = [
        data['Time'].strftime("%Y-%m-%d %H:%M:%S"),
        data['Buy'],
        data['Sell'],
        data['Volume']
    ]

    # Append the row
    sheet.append_row(row)

def main():
    try:
        # Parameters (modify as needed)
        item = "Prisma Case"
        skin = ""
        wear = 0
        stat = 0

        hash_name = get_hashname(item, skin, wear, stat)
        current_data = item_data(hash_name)
        current_data["Time"] = datetime.utcnow()

        # Append data to Google Sheets
        append_to_google_sheets(current_data)

        print('Data collected and appended successfully.')
    except Exception as e:
        print(f'An error occurred: {e}')
        raise e

if __name__ == "__main__":
    main()
