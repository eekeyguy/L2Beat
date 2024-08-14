import asyncio
import requests
from datetime import datetime
from airstack.execute_query import AirstackClient

# Airstack API configuration
airstack_api_key = "138959f866f104783a0ac885fc208eb78"
api_client = AirstackClient(api_key=airstack_api_key)

# Dune API configuration
dune_api_key = "3gE1dURYhPgEeOWE9hF39PQCNoLCBkcd"
dune_create_url = "https://api.dune.com/api/v1/table/create"
dune_upload_url = "https://api.dune.com/api/v1/table/upload/csv"

# Correct namespace
namespace = "blockintel"
table_name = "farcaster_moxie_claims"

query = """
query MyQuery($cursor: String) {
  FarcasterMoxieClaimDetails(
    input: {filter: {fid: {}}, blockchain: ALL, order: {availableClaimAmount: DESC}, limit: 200, cursor: $cursor}
  ) {
    FarcasterMoxieClaimDetails {
      availableClaimAmount
    }
    pageInfo {
      hasNextPage
      nextCursor
    }
  }
}
"""

async def fetch_page(cursor, retries=3):
    for attempt in range(retries):
        try:
            execute_query_client = api_client.create_execute_query_object(query=query, variables={"cursor": cursor})
            query_response = await execute_query_client.execute_query()
            if query_response and query_response.data:
                return query_response.data.get('FarcasterMoxieClaimDetails')
            else:
                print(f"Received empty response on attempt {attempt + 1}")
        except Exception as e:
            print(f"Error on attempt {attempt + 1}: {str(e)}")
        
        if attempt < retries - 1:
            wait_time = 2 ** attempt  # Exponential backoff
            print(f"Retrying in {wait_time} seconds...")
            await asyncio.sleep(wait_time)
    
    print("Failed to fetch page after all retries")
    return None

def create_dune_table():
    headers = {
        "X-DUNE-API-KEY": dune_api_key,
        "Content-Type": "application/json"
    }
    payload = {
        "namespace": namespace,
        "table_name": table_name,
        "description": "Farcaster Moxie claim amounts from Airstack API",
        "schema": [
            {"name": "date", "type": "timestamp"},
            {"name": "claim_amount", "type": "double"}
        ],
        "is_private": False
    }
    response = requests.post(dune_create_url, json=payload, headers=headers)
    if response.status_code == 200:
        print("Table created successfully on Dune")
        return True
    else:
        print(f"Failed to create table on Dune. Status code: {response.status_code}")
        print(f"Response: {response.text}")
        return False

def upload_to_dune(csv_data):
    headers = {
        "X-DUNE-API-KEY": dune_api_key,
    }
    payload = {
        "table_name": f"{namespace}.{table_name}",
    }
    files = {
        "file": ("data.csv", csv_data, "text/csv")
    }
    
    response = requests.post(dune_upload_url, headers=headers, data=payload, files=files)
    
    if response.status_code == 200:
        print("Data successfully uploaded to Dune")
    else:
        print(f"Failed to upload data to Dune. Status code: {response.status_code}")
        print(f"Response: {response.text}")

async def main():
    cursor = ""
    total_claim_amount = 0
    page_count = 0
    threshold = 50  # Threshold value
    data_to_upload = []
    
    while True:
        page_data = await fetch_page(cursor)
        
        if page_data is None:
            print("Encountered an error. Stopping the process.")
            break
        
        page_count += 1
        
        if not page_data['FarcasterMoxieClaimDetails']:
            print("No more data available. Stopping the process.")
            break
        
        first_amount = float(page_data['FarcasterMoxieClaimDetails'][0]['availableClaimAmount'])
        if first_amount < threshold:
            print(f"First availableClaimAmount on page {page_count} is less than {threshold}. Stopping the process.")
            break
        
        for detail in page_data['FarcasterMoxieClaimDetails']:
            amount = float(detail['availableClaimAmount'])
            if amount < threshold:
                break
            total_claim_amount += amount
            data_to_upload.append(f"{datetime.now().isoformat()},{amount}")
        
        if not page_data['pageInfo']['hasNextPage']:
            break
        
        cursor = page_data['pageInfo']['nextCursor']
    
    print(f"Total sum of availableClaimAmount (≥{threshold}) across {page_count} pages: {total_claim_amount}")
    
    # Create table on Dune
    if not create_dune_table():
        print("Aborting due to failure in table creation")
        return

    # Prepare data for Dune
    csv_data = "date,claim_amount\n" + "\n".join(data_to_upload)
    
    # Upload to Dune
    upload_to_dune(csv_data)

if __name__ == "__main__":
    asyncio.run(main())
