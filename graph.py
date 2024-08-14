import asyncio
import requests
from datetime import datetime
from airstack.execute_query import AirstackClient

# Airstack API configuration
airstack_api_key = "138959f866f104783a0ac885fc208eb78"
api_client = AirstackClient(api_key=airstack_api_key)

# Dune API configuration
dune_api_key = "3gE1dURYhPgEeOWE9hF39PQCNoLCBkcd"
dune_upload_url = "https://api.dune.com/api/v1/table/upload/csv"

# Correct namespace and table name
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

def upload_to_dune(unclaimed_amount):
    headers = {
        "X-DUNE-API-KEY": dune_api_key,
    }
    payload = {
        "table_name": f"{namespace}.{table_name}",
    }
    csv_data = f"date,unclaimed_amount\n{datetime.now().isoformat()},{unclaimed_amount}"
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
    total_unclaimed_amount = 0
    page_count = 0
    
    while True:
        page_data = await fetch_page(cursor)
        
        if page_data is None:
            print("Encountered an error. Stopping the process.")
            break
        
        page_count += 1
        
        if not page_data['FarcasterMoxieClaimDetails']:
            print("No more data available. Stopping the process.")
            break
        
        for detail in page_data['FarcasterMoxieClaimDetails']:
            amount = float(detail['availableClaimAmount'])
            total_unclaimed_amount += amount
        
        if not page_data['pageInfo']['hasNextPage']:
            break
        
        cursor = page_data['pageInfo']['nextCursor']
    
    print(f"Total unclaimed amount across {page_count} pages: {total_unclaimed_amount}")
    
    # Upload to Dune
    upload_to_dune(total_unclaimed_amount)

if __name__ == "__main__":
    asyncio.run(main())
