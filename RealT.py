import requests
import json
import csv
from io import StringIO
from datetime import datetime

def fetch_realt_data():
    url = "https://api.realt.community/v1/token"
    response = requests.get(url)
    return response.json()

def extract_token_data(json_data):
    extracted_data = []
    current_date = datetime.now().strftime('%Y-%m-%d')
    
    for token in json_data:
        extracted_data.append({
            'date': current_date,
            'full_name': token['fullName'],
            'short_name': token['shortName'],
            'symbol': token['symbol'],
            'product_type': token['productType'],
            'token_price': token['tokenPrice'],
            'currency': token['currency'],
            'ethereum_contract': token['ethereumContract'],
            'xdai_contract': token['xDaiContract'],
            'gnosis_contract': token['gnosisContract'],
            'last_update': token['lastUpdate']['date']
        })
    
    return extracted_data

def convert_to_csv(extracted_data):
    csv_file = StringIO()
    fieldnames = ['date', 'full_name', 'short_name', 'symbol', 'product_type', 'token_price', 'currency', 
                  'ethereum_contract', 'xdai_contract', 'gnosis_contract', 'last_update']
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
        "description": "RealT Token Data",
        "table_name": "realt_token_data",
        "is_private": False
    })
    headers = {
        'Content-Type': 'application/json',
        'X-DUNE-API-KEY': 'p0RZJpTPCUn9Cn7UTXEWDhalc53QzZXV'
    }
    response = requests.post(dune_upload_url, headers=headers, data=payload)
    print(response.text)

def main():
    # Fetch RealT data
    json_data = fetch_realt_data()
    
    # Extract token data
    extracted_data = extract_token_data(json_data)
    
    # Convert to CSV
    csv_data = convert_to_csv(extracted_data)
    
    # Upload to Dune
    upload_to_dune(csv_data)

if __name__ == "__main__":
    main()
