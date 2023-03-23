import asyncio
import os
import argparse
from pprint import pprint

from assemblyline_client import get_client

from .client import Client


HAUNTED_HOUSE_URL = os.environ['HAUNTEDHOUSE_URL']
HAUNTED_HOUSE_API_KEY = os.environ['HAUNTEDHOUSE_API_KEY']

ASSEMBLYLINE_URL = os.environ['ASSEMBLYLINE_URL']
ASSEMBLYLINE_USER = os.environ['ASSEMBLYLINE_USER']
ASSEMBLYLINE_API_KEY = os.environ['ASSEMBLYLINE_API_KEY']


async def main(yara_file, access, verify):

    # Read the signature we are going to search with
    with open(yara_file) as handle:
        body = handle.read()

    # Get the
    al_client = get_client(ASSEMBLYLINE_URL, apikey=(ASSEMBLYLINE_USER, ASSEMBLYLINE_API_KEY))
    classification_definition = al_client._connection.get('api/v4/help/classification_definition')
    classification_definition = classification_definition['original_definition']

    async with Client(HAUNTED_HOUSE_URL, HAUNTED_HOUSE_API_KEY,
                      classification=classification_definition, verify=verify) as client:

        search_status = await client.start_search(body, access, archive_only=False)
        pprint(search_status)

        while not search_status.stage.lower() == 'finished':
            await asyncio.sleep(10)
            search_status = await client.search_status(search_status.code, access)
            pprint(search_status)

        if search_status.errors:
            print(search_status.errors)

        if search_status.hits:
            print(search_status.hits)
        else:
            print("No hits")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        prog='search',
        description='run a search against a hauntedhouse server',
    )
    parser.add_argument("yara_file")
    parser.add_argument("--trust-all", help="ignore server verification", action='store_true')
    parser.add_argument("--access")
    args = parser.parse_args()

    asyncio.run(main(args.yara_file, args.access, verify=not args.trust_all))

