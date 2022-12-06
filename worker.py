import sys
import time
import logging
import tempfile
import asyncio

from haunted_house import WorkerBuilder


async def main():
    logger = logging.getLogger("haunted_house")
    logger.handlers.clear()
    stream = logging.StreamHandler(sys.stdout)
    stream.formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    logger.addHandler(stream)
    logger.setLevel(logging.DEBUG)


    with tempfile.TemporaryDirectory() as tempdir:
        builder = WorkerBuilder()
        builder.api_token('password')
        builder.cache_directory(tempdir, 100 << 30, 2 << 30)
        worker = await builder.start()

        while True:
            time.sleep(2)

if __name__ == '__main__':
    asyncio.run(main())
