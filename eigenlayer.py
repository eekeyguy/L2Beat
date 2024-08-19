import requests
import json
import csv
from io import StringIO
from datetime import datetime, timedelta

def fetch_eigenlayer_data():
    url = "https://api.llama.fi/protocol/eigenlayer"
    response = requests.get(url)
    return response.json()

def extract_tvl_data(json_data):
    tvl_data = json_data['chainTvls']['Ethereum']['tvl']
    
    # Get the last 90 days of data
    ninety_days_ago = datetime.now() - timedelta(days=90)
    filtered_data = [
        {
            'date': datetime.fromtimestamp(entry['date']).strftime('%Y-%m-%d'),
            'tvl': entry['totalLiquidityUSD']
        }
        for entry in tvl_data
        if datetime.fromtimestamp(entry['date']) >= ninety_days_ago
    ]
    
    return filtered_data

def convert_to_csv(extracted_data):
    csv_file = StringIO()
    fieldnames = ['date', 'tvl']
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
        "description": "EigenLayer TVL Data",
        "table_name": "eigenlayer_tvl",
        "is_private": False
    })
    headers = {
        'Content-Type': 'application/json',
        'X-DUNE-API-KEY': 'p0RZJpTPCUn9Cn7UTXEWDhalc53QzZXV'
    }
    response = requests.post(dune_upload_url, headers=headers, data=payload)
    print(response.text)

def main():
    # Fetch data from EigenLayer API
    json_data = fetch_eigenlayer_data()
    
    # Extract TVL data
    extracted_data = extract_tvl_data(json_data)
    
    # Convert to CSV
    csv_data = convert_to_csv(extracted_data)
    
    # Upload to Dune
    upload_to_dune(csv_data)

if __name__ == "__main__":
    main()
