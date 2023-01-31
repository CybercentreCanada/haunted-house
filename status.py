from pprint import pprint
import asyncio

import config
import aiohttp


async def main():

    async with aiohttp.ClientSession(headers={'Authorization': 'Bearer ' + config.HAUNTEDHOUSE_KEY}) as session:
        async with session.get(config.HAUNTEDHOUSE_URL + "/status/detailed") as resp:
            resp.raise_for_status()
            pprint(await resp.json())


if __name__ == '__main__':
    asyncio.run(main())
