import requests
import json
import csv
import io

# Fetch data from the API
api_url = "https://api.nearblocks.io/v1/charts"
api_response = requests.get(api_url)
api_data = api_response.json()['charts']

# Convert the data to CSV format
csv_file = io.StringIO()
csv_writer = csv.writer(csv_file)
csv_writer.writerow(['date', 'near_price', 'market_cap', 'total_supply', 'blocks', 'gas_fee', 'gas_used', 'avg_gas_price', 'avg_gas_limit', 'txns', 'txn_volume', 'txn_volume_usd', 'txn_fee', 'txn_fee_usd', 'total_addresses', 'addresses'])

for item in api_data:
    csv_writer.writerow([item['date'], item['near_price'], item['market_cap'], item['total_supply'], item['blocks'], item['gas_fee'], item['gas_used'], item['avg_gas_price'], item['avg_gas_limit'], item['txns'], item['txn_volume'], item['txn_volume_usd'], item['txn_fee'], item['txn_fee_usd'], item['total_addresses'], item['addresses']])

csv_data = csv_file.getvalue()
csv_file.close()

# Upload the CSV data to Dune
dune_url = "https://api.dune.com/api/v1/table/upload/csv"
dune_payload = json.dumps({
    "data": csv_data,
    "description": "NEARBlocks Chart Data",
    "table_name": "NEARBlocks_Chart",
    "is_private": False
})
dune_headers = {
    'Content-Type': 'application/json',
    'X-DUNE-API-KEY': 'BSka2d5Pb7N18wuCuxFo37RK5NTBAcKa'  # Replace with your actual Dune API key
}

dune_response = requests.request("POST", dune_url, headers=dune_headers, data=dune_payload)
print(dune_response.text)
