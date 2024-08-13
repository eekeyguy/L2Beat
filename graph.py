import asyncio
from airstack.execute_query import AirstackClient

api_client = AirstackClient(api_key="138959f866f104783a0ac885fc208eb78")

query = """query MyQuery {
  FarcasterMoxieClaimDetails(
    input: {filter: {fid: {}}, blockchain: ALL, order: {availableClaimAmount: DESC}}
  ) {
    FarcasterMoxieClaimDetails {
      availableClaimAmount
    }
  }
}"""

async def main():
    execute_query_client = api_client.create_execute_query_object(query=query)
    query_response = await execute_query_client.execute_query()
    
    # Extract the list of dictionaries containing availableClaimAmount
    claim_details = query_response.data['FarcasterMoxieClaimDetails']['FarcasterMoxieClaimDetails']
    
    # Calculate the sum of all availableClaimAmount values
    total_claim_amount = sum(float(detail['availableClaimAmount']) for detail in claim_details)
    
    print(f"Sum of all availableClaimAmount: {total_claim_amount}")

asyncio.run(main())
