import asyncio
import json
import requests
from airstack.execute_query import AirstackClient

# Airstack API setup
api_client = AirstackClient(api_key="138959f866f104783a0ac885fc208eb78")
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

async def main():
    cursor = ""
    threshold = 25  # Threshold value
    total_claim_amount = 0
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
        
        first_amount = float(page_data['FarcasterMoxieClaimDetails'][0]['availableClaimAmount'])
        if first_amount < threshold:
            print(f"First availableClaimAmount on page {page_count} is less than {threshold}. Stopping the process.")
            break
        
        for detail in page_data['FarcasterMoxieClaimDetails']:
            amount = float(detail['availableClaimAmount'])
            if amount < threshold:
                break
            total_claim_amount += amount
        
        if not page_data['pageInfo']['hasNextPage']:
            break
        cursor = page_data['pageInfo']['nextCursor']

    print(f"Total sum of availableClaimAmount (≥{threshold}) across {page_count} pages: {total_claim_amount}")

    # Prepare and send data to Dune
    dune_url = "https://api.dune.com/api/v1/table/upload/csv"
    csv_data = f"TotalAvailableClaimAmount\n{total_claim_amount}"
    payload = json.dumps({
        "data": csv_data,
        "description": "Total Farcaster Moxie Claim Amount",
        "table_name": "FarcasterMoxieTotalClaim",
        "is_private": False
    })
    headers = {
        'Content-Type': 'application/json',
        'X-DUNE-API-KEY': 'SXeoX53Eo86VXSas2ohpHLq18waV5MCo'  # Replace with your actual Dune API key
    }
    dune_response = requests.post(dune_url, headers=headers, data=payload)
    print("Dune API Response:", dune_response.text)

if __name__ == "__main__":
    asyncio.run(main())
