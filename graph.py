import asyncio
import time
from airstack.execute_query import AirstackClient

api_client = AirstackClient(api_key="138959f866f104783a0ac885fc208eb78")

query = """
query MyQuery($cursor: String) {
  FarcasterMoxieClaimDetails(
    input: {filter: {fid: {}}, blockchain: ALL, order: {claimedAmount: DESC}, limit: 200, cursor: $cursor}
  ) {
    FarcasterMoxieClaimDetails {
      claimedAmount
      fid
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
    total_claimed_amount = 0
    cursor = ""
    has_next_page = True
    page_count = 0

    while has_next_page:
        page_data = await fetch_page(cursor)
        
        if page_data is None:
            print("Encountered an error. Stopping the process.")
            break

        # Sum up claimedAmount for current page
        page_total = sum(float(detail['claimedAmount']) for detail in page_data['FarcasterMoxieClaimDetails'])
        total_claimed_amount += page_total
        page_count += 1

        # Update cursor and has_next_page for next iteration
        has_next_page = page_data['pageInfo']['hasNextPage']
        cursor = page_data['pageInfo']['nextCursor'] if has_next_page else None

    print(f"Total sum of all claimedAmount: {total_claimed_amount}")
    print(f"Total pages processed: {page_count}")

if __name__ == "__main__":
    asyncio.run(main())
