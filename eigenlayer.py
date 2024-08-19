import requests
import json
import csv
from io import StringIO
from datetime import datetime, timedelta
from collections import OrderedDict

def fetch_eigenlayer_data():
    url = "https://api.llama.fi/protocol/eigenlayer"
    response = requests.get(url)
    return response.json()

def extract_tokens_usd_data(json_data):
    tokens_usd_data = json_data['chainTvls']['Ethereum']['tokensInUsd']
    
    # Get the last 90 days of data
    ninety_days_ago = datetime.now() - timedelta(days=90)
    filtered_data = []
    all_tokens = set()
    
    for entry in tokens_usd_data:
        entry_date = datetime.fromtimestamp(entry['date'])
        if entry_date >= ninety_days_ago:
            filtered_entry = OrderedDict([('date', entry_date.strftime('%Y-%m-%d'))])
            for token, value in entry['tokens'].items():
                filtered_entry[token] = value
                all_tokens.add(token)
            filtered_data.append(filtered_entry)
    
    # Ensure all entries have all tokens (with 0 as default value)
    for entry in filtered_data:
        for token in all_tokens:
            if token not in entry:
                entry[token] = 0
    
    return filtered_data, list(all_tokens)

def convert_to_csv(extracted_data, token_list):
    csv_file = StringIO()
    fieldnames = ['date'] + token_list
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
        "description": "EigenLayer Tokens in USD Data (All Tokens)",
        "table_name": "eigenlayer_tokens_usd_all",
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
    
    # Extract Tokens in USD data
    extracted_data, token_list = extract_tokens_usd_data(json_data)
    
    # Convert to CSV
    csv_data = convert_to_csv(extracted_data, token_list)
    
    # Upload to Dune
    upload_to_dune(csv_data)

if __name__ == "__main__":
    main()
