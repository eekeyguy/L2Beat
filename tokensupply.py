import requests
import json
import csv
from io import StringIO
from datetime import datetime

def fetch_market_data():
    url = "https://api.coingecko.com/api/v3/coins/markets"
    params = {
        "vs_currency": "usd",
        "ids": "ethereum,rocket-pool-eth,staked-ether,coinbase-wrapped-staked-eth,stakewise-v3-oseth,sweth,origin-ether,wrapped-beacon-eth,ankreth,stader-ethx,staked-frax-ether,liquid-staked-ethereum,mantle-staked-ether",
        "per_page": 100,
        "page": 1,
        "sparkline": "false",
        "price_change_percentage": "1h,24h,7d",
        "locale": "en"
    }
    response = requests.get(url, params=params)
    return response.json()

def extract_circulating_supply_data(json_data):
    extracted_data = []
    current_date = datetime.now().strftime('%Y-%m-%d')
    
    for coin in json_data:
        extracted_data.append({
            'date': current_date,
            'coin_id': coin['id'],
            'circulating_supply': coin['circulating_supply']
        })
    
    return extracted_data

def convert_to_csv(extracted_data):
    csv_file = StringIO()
    fieldnames = ['date', 'coin_id', 'circulating_supply']
    csv_writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
    csv_writer.writeheader()
    csv_writer.writerows(extracted_data)
    csv_data = csv_file.getvalue()
    csv_file.close()
    return csv_data

def upload_to_dune(csv_data):
    dune_upload_url = "https://api.dune.com/api/v1/table/upload/csv"
    payload = json.dumps({
        "data": csv_data,
        "description": "Circulating Supply Data",
        "table_name": "circulating_supply",
        "is_private": False
    })
    headers = {
        'Content-Type': 'application/json',
        'X-DUNE-API-KEY': 'p0RZJpTPCUn9Cn7UTXEWDhalc53QzZXV'
    }
    response = requests.post(dune_upload_url, headers=headers, data=payload)
    print(response.text)

def main():
    # Fetch market data
    json_data = fetch_market_data()
    
    # Extract circulating supply data
    extracted_data = extract_circulating_supply_data(json_data)
    
    # Convert to CSV
    csv_data = convert_to_csv(extracted_data)
    
    # Upload to Dune
    upload_to_dune(csv_data)

if __name__ == "__main__":
    main()
