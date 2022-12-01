import sys
import logging
import tempfile
import time
import os
import os.path
import base64
import asyncio

from haunted_house import ServerBuilder


async def main():
    logger = logging.getLogger("haunted_house")
    logger.handlers.clear()
    logger.addHandler(logging.StreamHandler(sys.stdout))
    logger.setLevel(logging.DEBUG)

    with tempfile.TemporaryDirectory() as tempdir:
        try:
            builder = ServerBuilder()
            builder.static_authentication({
                'password': {'search', 'worker'}
            })
            builder.batch_limit_size(10)
            builder.batch_limit_seconds(10)
            builder.cache_directory(tempdir, 100 << 30)
            server = await builder.build()

            async def print_status():
                status = await server.status()
                print(status.indices)
                print(status.ingest_buffer)
                print(status.ingest_batch_active)


            while True:

                directories = ["."]
                while directories:
                    current = directories.pop()

                    for file in os.listdir(current):
                        file = os.path.join(current, file)
                        if os.path.isdir(file):
                            directories.append(file)

                        if os.path.isfile(file):
                            print("upload", file)
                            hash = bytes(await server.upload(file))
                            print(base64.b64encode(hash))
                            await server.ingest_file(hash, "", None)

                            await print_status()
        finally:
            await asyncio.sleep(3)


if __name__ == '__main__':
    asyncio.run(main())
