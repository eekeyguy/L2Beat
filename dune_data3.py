import requests
import json
import csv
from io import StringIO

# Custom JSON decoder that converts null to None
def custom_json_decoder(pairs):
    return {k: (v if v is not None else None) for k, v in pairs}

# Function to extract required fields from the JSON data
def extract_data(json_data):
    extracted_data = []
    for key, value in json_data['data'].items():
        quote = value['quote']['USD']
        extracted_data.append({
            'id': value['id'],
            'name': value['name'],
            'symbol': value['symbol'],
            'circulating_supply': value['circulating_supply'],
            'total_supply': value['total_supply'],
            'price': quote['price'],
            'volume_24h': quote['volume_24h'],
            'volume_change_24h': quote['volume_change_24h'],
            'percent_change_1h': quote['percent_change_1h'],
            'percent_change_24h': quote['percent_change_24h'],
            'percent_change_7d': quote['percent_change_7d'],
            'percent_change_30d': quote['percent_change_30d'],
            'percent_change_60d': quote['percent_change_60d'],
            'percent_change_90d': quote['percent_change_90d'],
            'market_cap': quote['market_cap'],
            'market_cap_dominance': quote['market_cap_dominance'],
            'fully_diluted_market_cap': quote['fully_diluted_market_cap']
        })
    return extracted_data

# Function to convert extracted data to CSV format
def convert_to_csv(extracted_data):
    csv_file = StringIO()
    fieldnames = ['id', 'name', 'symbol', 'circulating_supply', 'total_supply', 'price', 'volume_24h', 'volume_change_24h', 'percent_change_1h', 'percent_change_24h', 'percent_change_7d', 'percent_change_30d', 'percent_change_60d', 'percent_change_90d', 'market_cap', 'market_cap_dominance', 'fully_diluted_market_cap']
    csv_writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
    csv_writer.writeheader()
    csv_writer.writerows(extracted_data)
    csv_data = csv_file.getvalue()
    csv_file.close()
    return csv_data

# Function to upload CSV data to Dune
def upload_to_dune(csv_data):
    dune_upload_url = "https://api.dune.com/api/v1/table/upload/csv"
    payload = json.dumps({
        "data": csv_data,
        "description": "Data",
        "table_name": "coins_data",
        "is_private": False
    })
    headers = {
        'Content-Type': 'application/json',
        'X-DUNE-API-KEY': 'Erqr0LWuBjAH5rE2eq7WJpspXNJeuCIM'  # Replace with your actual Dune API key
    }

    # Send POST request to Dune API
    response = requests.request("POST", dune_upload_url, headers=headers, data=payload)
    print(response.text)

# Main function
def main():
    url = "https://pro-api.coinmarketcap.com/v2/cryptocurrency/quotes/latest?id=6535,22861,7411,6841,541,4191,27766,6747,5690,2280,2416,5632,7431,5665,1720,9104,3911,2777,3640,2682,3029,1042,1455,16612,3783,1027,5426,1839,2010,5805,11419,1958,3794,4642,7226,20947,23149,4030,21794,21159,6536,6748,9481,5198,20362,4950,21516,4092,10603,11840,27075,11841,22691,13631,9640,1934,14556,28932,3978,7229,27445,7083,8916,1518,7278,2586,12387,10791,7186,6538,13855,5692,6945,7080,10804,28298,6783,6210,18876,14101,23711,8719,2130,29335,18069,5691,28933,74,5994,24478,28752,23095,26873,29870,28301,25028,28194,28412,28920,25208,29095"
    headers = {
        'X-CMC_PRO_API_KEY': 'ab201778-8ee5-4400-ae2d-79925af07c88',
        'Accept': '*/*'
    }

    # Fetch data from the API
    response = requests.get(url, headers=headers)
    json_data = response.text

    # Load JSON data with custom decoder
    data = json.loads(json_data, object_pairs_hook=custom_json_decoder)

    # Extracting data
    extracted_data = extract_data(data)

    # Converting to CSV
    csv_data = convert_to_csv(extracted_data)

    # Uploading to Dune
    upload_to_dune(csv_data)

if __name__ == "__main__":
    main()
