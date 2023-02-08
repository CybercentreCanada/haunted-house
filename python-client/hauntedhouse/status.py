from pprint import pprint
import asyncio
import os

import aiohttp


HAUNTED_HOUSE_URL = os.environ['HAUNTEDHOUSE_URL']
HAUNTED_HOUSE_API_KEY = os.environ['HAUNTEDHOUSE_API_KEY']


async def main():

    async with aiohttp.ClientSession(headers={'Authorization': 'Bearer ' + HAUNTED_HOUSE_API_KEY}) as session:
        async with session.get(HAUNTED_HOUSE_URL + "/status/detailed") as resp:
            resp.raise_for_status()
            pprint(await resp.json())


if __name__ == '__main__':
    asyncio.run(main())
