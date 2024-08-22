import requests
import json
import csv
from io import StringIO
from datetime import datetime

def fetch_etf_netflow_data():
    url = "https://api.coinmarketcap.com/data-api/v3/etf/detail/netflow?category=btc&range=30d"
    response = requests.get(url)
    return response.json()

def extract_netflow_data(json_data):
    extracted_data = []
    for etf in json_data['data']['points']:
        etf_name = etf['name']
        for point in etf['data']:
            timestamp = datetime.fromtimestamp(int(point['timestamp']) / 1000).strftime('%Y-%m-%d')
            extracted_data.append({
                'date': timestamp,
                'etf_name': etf_name,
                'net_flow': point['value']
            })
    return extracted_data

def convert_to_csv(extracted_data):
    csv_file = StringIO()
    fieldnames = ['date', 'etf_name', 'net_flow']
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
        "description": "30-Day ETF Net Flow Data",
        "table_name": "etf_net_flow_30d",
        "is_private": False
    })
    headers = {
        'Content-Type': 'application/json',
        'X-DUNE-API-KEY': 'YOUR_DUNE_API_KEY_HERE'
    }
    response = requests.post(dune_upload_url, headers=headers, data=payload)
    print(response.text)

def main():
    # Fetch 30-day ETF net flow data
    json_data = fetch_etf_netflow_data()
    
    # Extract net flow data
    extracted_data = extract_netflow_data(json_data)
    
    # Convert to CSV
    csv_data = convert_to_csv(extracted_data)
    
    # Upload to Dune
    upload_to_dune(csv_data)

if __name__ == "__main__":
    main()
