import sys
import logging
import tempfile
import os
import os.path
import asyncio

from haunted_house import ServerBuilder


async def main():
    logger = logging.getLogger("haunted_house")
    logger.handlers.clear()
    stream = logging.StreamHandler(sys.stdout)
    stream.formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    logger.addHandler(stream)
    logger.setLevel(logging.INFO)

    with tempfile.TemporaryDirectory() as tempdir:
        builder = ServerBuilder()
        builder.static_authentication({
            'password': {'search', 'worker'}
        })
        builder.batch_limit_size(10)
        builder.batch_limit_seconds(1)
        # builder.index_soft_entries_max(5)
        builder.cache_directory(tempdir, 100 << 30)
        server = await builder.build()

        # async def print_status():
        #     status = await server.status()
        #     print(status.indices)
        #     print(status.ingest_buffer)
        #     print(status.ingest_batch_active)

        futures = []
        directories = ["."]
        while directories and len(futures) < 100:
            current = directories.pop()

            for file in os.listdir(current):
                file = os.path.join(current, file)
                if os.path.isdir(file):
                    directories.append(file)

                if os.path.isfile(file):
                    hash = bytes(await server.upload(file))
                    futures.append(server.ingest_file(hash, "", None))
                    if len(futures) > 100:
                        break

        for file in asyncio.as_completed(futures):
            await file

        while True:
            await asyncio.sleep(3)


if __name__ == '__main__':
    asyncio.run(main())
