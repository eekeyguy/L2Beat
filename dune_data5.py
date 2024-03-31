import requests
from datetime import datetime, timedelta
import csv
import io
import json

# List of chains
chains = ["arbitrum", "optimism", "base", "blast", "starknet", "zksync era", "zora", "mantle", "metis", "linea", "dydx", "mode"]

# Base API endpoint
base_url = "https://api.llama.fi/v2/historicalChainTvl/"

# Get today's date in UTC
today = datetime.utcnow()

# Calculate the date for 10 days ago in UTC
ten_days_ago = today - timedelta(days=10)

# Create an in-memory file-like object to store the CSV data
output = io.StringIO()
fieldnames = ['chain', 'date', 'tvl']
writer = csv.DictWriter(output, fieldnames=fieldnames)
writer.writeheader()

# Iterate over the list of chains and fetch data for each chain
for chain in chains:
    # Format the URL with the current chain
    url = base_url + chain.replace(" ", "%20")  # Replace spaces with %20 for URL encoding

    # Send the request
    response = requests.get(url)
    data = response.json()

    # Filter the last 10 days data
    filtered_data = [{
        'date': datetime.utcfromtimestamp(item['date']).strftime('%Y-%m-%d %H:%M:%S'),
        'tvl': item['tvl']
    } for item in data if datetime.utcfromtimestamp(item['date']) >= ten_days_ago]

    # Write the filtered data to the CSV string
    for row in filtered_data:
        writer.writerow({'chain': chain, 'date': row['date'], 'tvl': row['tvl']})

# Convert the CSV data in the StringIO object to a string
csv_data = output.getvalue()

# Send the CSV data to Dune
dune_url = "https://api.dune.com/api/v1/table/upload/csv"
payload = json.dumps({
    "data": csv_data,
    "description": "TVL Data",
    "table_name": "ActualTVL",
    "is_private": False
})
headers = {
    'Content-Type': 'application/json',
    'X-DUNE-API-KEY': 'xoGf7WxpXnzJtasQbuYEhTqaGTiYZl2a'  # Replace with your actual Dune API key
}

dune_response = requests.post(dune_url, headers=headers, data=payload)
print(dune_response.text)
