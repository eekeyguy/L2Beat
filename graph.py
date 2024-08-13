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
}""" # Replace with GraphQL Query

async def main():
    execute_query_client = api_client.create_execute_query_object(
        query=query)

    query_response = await execute_query_client.execute_query()
    print(query_response.data)

asyncio.run(main())
