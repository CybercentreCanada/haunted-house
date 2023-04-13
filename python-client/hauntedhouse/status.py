from pprint import PrettyPrinter
import asyncio
import os
import argparse

import aiohttp


HAUNTED_HOUSE_URL = os.environ['HAUNTEDHOUSE_URL']
HAUNTED_HOUSE_API_KEY = os.environ['HAUNTEDHOUSE_API_KEY']


async def main(verify):

    printer = PrettyPrinter(width=100, compact=True, underscore_numbers=True)

    async with aiohttp.ClientSession(headers={'Authorization': 'Bearer ' + HAUNTED_HOUSE_API_KEY}) as session:
        async with session.get(HAUNTED_HOUSE_URL + "/status/detailed", verify_ssl=verify) as resp:
            resp.raise_for_status()
            printer.pprint(await resp.json())


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        prog='status',
        description='print the server status page',
    )
    parser.add_argument("--trust-all", help="ignore server verification", action='store_true')
    args = parser.parse_args()

    asyncio.run(main(verify=not args.trust_all))
