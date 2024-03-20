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
    url = "https://pro-api.coinmarketcap.com/v2/cryptocurrency/quotes/latest?id=74,541,1027,1042,1455,1518,1720,1839,1934,1958,2010,2130,2280,2416,2424,2467,2586,2682,2777,3029,3640,3773,3783,3794,3911,3978,4030,4092,4191,4642,4950,5198,5426,5632,5665,5690,5691,5692,5805,5994,6210,6535,6536,6538,6747,6748,6783,6841,6945,7080,7083,7186,7226,7229,7278,7411,7431,8719,8916,9104,9481,9640,10603,10791,10804,11419,11840,11841,12387,13631,13855,14101,14556,16612,18069,18876,19055,20362,20947,21159,21516,21794,22691,22861,22974,23095,23149,23711,23756,24478,25028,25208,26590,26873,27075,27178,27445,27565,27766,28194,28298,28301,28412,28567,28752,28920,28932,28933,29095,29335,29870,8534,13967,9417"
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
