import requests
from datetime import datetime, timedelta
import json

# List of chains
chains = ["arbitrum", "optimism", "base", "blast", "starknet", "zksync-era", "zora", "mantle", "metis", "linea", "dydx", "mode"]

# Base API endpoint
base_url = "https://l2beat.com/api/tvl/"

# Get today's date in UTC and set the time to 12 AM
today_midnight = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)

# Calculate the timestamp for 31 days ago at 12 AM UTC
thirty_one_days_ago_midnight = today_midnight - timedelta(days=10)

# Initialize the CSV data string
csv_data = "Chain,Date (UTC),Value USD,CBV USD,EBV USD,NMV USD\n"

# Iterate over the list of chains
for chain in chains:
    # Construct the URL for the current chain
    url = base_url + chain.replace(" ", "%20") + ".json"  # Replace spaces with %20 for URL encoding

    # Send the request
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()['hourly']['data']

        # Iterate through the data and filter for the last 31 days at 12 AM UTC
        for entry in data:
            timestamp = entry[0]
            entry_date = datetime.utcfromtimestamp(timestamp)
            if thirty_one_days_ago_midnight <= entry_date < today_midnight and entry_date.hour == 0:
                # Append the filtered data to the CSV data string
                csv_data += f"{chain},{entry_date.strftime('%Y-%m-%d %H:%M:%S UTC')},{entry[1]},{entry[2]},{entry[3]},{entry[4]}\n"
    else:
        print(f"Error fetching data for {chain}: {response.status_code}")

# Send the CSV data to Dune
dune_url = "https://api.dune.com/api/v1/table/upload/csv"
payload = json.dumps({
    "data": csv_data,
    "description": "L2beat TVL Data",
    "table_name": "TVL",
    "is_private": False
})
headers = {
    'Content-Type': 'application/json',
    'X-DUNE-API-KEY': 'xoGf7WxpXnzJtasQbuYEhTqaGTiYZl2a'  # Replace with your actual Dune API key
}

dune_response = requests.post(dune_url, headers=headers, data=payload)
print(dune_response.text)
