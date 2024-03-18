import requests
import json
from datetime import datetime, timedelta

networks = [
    "arbitrum", "optimism", "blast", "mantapacific", "starknet", "base",
    "metis", "zksync-era", "mantle", "linea", "dydx", "immutablex", "zkfair",
    "polygonzkevm", "mode", "loopring", "scroll", "zksync-lite", "apex", "aevo"
]

def get_timestamps(data):
    latest_timestamp = data[-1][0]
    previous_day_timestamp = latest_timestamp - 24 * 60 * 60
    seven_days_ago_timestamp = latest_timestamp - 7 * 24 * 60 * 60
    return previous_day_timestamp, seven_days_ago_timestamp

def get_closest_values_for_timestamp(data, timestamp):
    closest_values = None
    min_diff = float('inf')
    for entry in data:
        diff = abs(entry[0] - timestamp)
        if diff < min_diff:
            min_diff = diff
            closest_values = entry[1:]
    return closest_values

def calculate_percentage_change(old_value, new_value):
    if old_value is None or new_value is None or old_value == 0:
        return None
    return ((new_value - old_value) / old_value) * 100

csv_data = "Network,Current Time,Current Value USD,Previous Day Value USD,Seven Days Ago Value USD,Daily Change (%),Weekly Change (%),CBV USD,EVB USD,NMV USD,CBV Daily Change (%),CBV Weekly Change (%),EBV Daily Change (%),EBV Weekly Change (%),NMV Daily Change (%),NMV Weekly Change (%)\n"

for network in networks:
    url = f"https://l2beat.com/api/tvl/{network}.json"
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()['hourly']['data']

        previous_day_timestamp, seven_days_ago_timestamp = get_timestamps(data)
        previous_day_values = get_closest_values_for_timestamp(data, previous_day_timestamp)
        seven_days_ago_values = get_closest_values_for_timestamp(data, seven_days_ago_timestamp)
        current_values = data[-1][1:]  # Assume the last entry is the most recent
        current_time = datetime.utcfromtimestamp(data[-1][0]).strftime('%Y-%m-%d %H:%M:%S')

        daily_change = calculate_percentage_change(previous_day_values[0], current_values[0])
        weekly_change = calculate_percentage_change(seven_days_ago_values[0], current_values[0])

        cbv_daily_change = calculate_percentage_change(previous_day_values[1], current_values[1])
        cbv_weekly_change = calculate_percentage_change(seven_days_ago_values[1], current_values[1])

        ebv_daily_change = calculate_percentage_change(previous_day_values[2], current_values[2])
        ebv_weekly_change = calculate_percentage_change(seven_days_ago_values[2], current_values[2])

        nmv_daily_change = calculate_percentage_change(previous_day_values[3], current_values[3])
        nmv_weekly_change = calculate_percentage_change(seven_days_ago_values[3], current_values[3])

        csv_data += f"{network},{current_time},{current_values[0]},{previous_day_values[0]},{seven_days_ago_values[0]},{daily_change or 'N/A'},{weekly_change or 'N/A'},{current_values[1]},{current_values[2]},{current_values[3]},{cbv_daily_change or 'N/A'},{cbv_weekly_change or 'N/A'},{ebv_daily_change or 'N/A'},{ebv_weekly_change or 'N/A'},{nmv_daily_change or 'N/A'},{nmv_weekly_change or 'N/A'}\n"
    except Exception as e:
        print(f"Error fetching data for {network}: {e}")

# Send the data to Dune
url = "https://api.dune.com/api/v1/table/upload/csv"
payload = json.dumps({
    "data": csv_data,
    "description": "L2beat TVL Data",
    "table_name": "TVL",
    "is_private": False
})
headers = {
    'Content-Type': 'application/json',
    'X-DUNE-API-KEY': '6k3Iy6Fn03geArlgjnmg2b1CkTFE3Kll'  # Replace 'Your-Dune-API-Key' with your actual Dune API key
}

response = requests.request("POST", url, headers=headers, data=payload)
print(response.text)
