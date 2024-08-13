import asyncio
from airstack.execute_query import AirstackClient

api_client = AirstackClient(api_key="138959f866f104783a0ac885fc208eb78")

query = """
query MyQuery($cursor: String) {
  FarcasterMoxieClaimDetails(
    input: {filter: {fid: {}}, blockchain: ALL, limit: 200, cursor: $cursor}
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

async def fetch_page(cursor):
    execute_query_client = api_client.create_execute_query_object(query=query, variables={"cursor": cursor})
    query_response = await execute_query_client.execute_query()
    return query_response.data['FarcasterMoxieClaimDetails']

async def main():
    total_claimed_amount = 0
    cursor = ""
    has_next_page = True

    while has_next_page:
        page_data = await fetch_page(cursor)
        
        # Sum up claimedAmount for current page
        page_total = sum(float(detail['claimedAmount']) for detail in page_data['FarcasterMoxieClaimDetails'])
        total_claimed_amount += page_total

        # Update cursor and has_next_page for next iteration
        has_next_page = page_data['pageInfo']['hasNextPage']
        cursor = page_data['pageInfo']['nextCursor'] if has_next_page else None

        print(f"Processed page. Current total: {total_claimed_amount}")

    print(f"Total sum of all claimedAmount: {total_claimed_amount}")

asyncio.run(main())
