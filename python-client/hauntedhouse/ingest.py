


# def pretty_classification(item):
#     if isinstance(item, str):
#         return item
#     if 'And' in item:
#         parts = [pretty_classification(x) for x in item["And"]]
#         return 'And(' + ', '.join(parts) + ')'
#     if 'Or' in item:
#         parts = [pretty_classification(x) for x in item["Or"]]
#         return 'Or(' + ', '.join(parts) + ')'
#     if 'Token' in item:
#         return '"' + item['Token'].replace('"', '\\"') + '"'
#     raise NotImplementedError()

import argparse
import json
import asyncio
import time
import os

from assemblyline_client import get_client

from .client import Client, DuplicateToken


FL = 'classification,sha256,expiry_ts,_seq_no,_primary_term'


ASSEMBLYLINE_URL = os.environ['ASSEMBLYLINE_URL']
ASSEMBLYLINE_USER = os.environ['ASSEMBLYLINE_USER']
ASSEMBLYLINE_API_KEY = os.environ['ASSEMBLYLINE_API_KEY']

HAUNTED_HOUSE_URL = os.environ['HAUNTEDHOUSE_URL']
HAUNTED_HOUSE_API_KEY = os.environ['HAUNTEDHOUSE_API_KEY']

BATCH_SIZE = int(os.getenv('AL_BATCH_SIZE', '1000'))


async def socket_main(verify) -> None:
    al_client = get_client(ASSEMBLYLINE_URL, apikey=(ASSEMBLYLINE_USER, ASSEMBLYLINE_API_KEY))

    classification_definition = al_client._connection.get('api/v4/help/classification_definition')

    last_seq_no = -1
    completed_seq_no = -1
    successful_search = False

    recent_tokens: dict[str, float] = {}
    current_sequence_numbers: list[tuple[int, int]] = []
    waiting_sequence_numbers: list[tuple[int, int]] = []
    futures: set[asyncio.Future[str]] = set()

    async with Client(HAUNTED_HOUSE_URL, HAUNTED_HOUSE_API_KEY, classification_definition['original_definition'], verify=verify) as house_client:
        assert house_client.access_engine.enforce

        while True:

            # Process any completed ingestion, moving the cursor for completed sequence numbers ahead
            if futures:
                if not successful_search:
                    done, futures = await asyncio.wait(futures, timeout=30, return_when=asyncio.FIRST_COMPLETED)
                else:
                    done = set([f for f in futures if f.done()])
                    futures = futures - done

                for future in done:
                    _term, _seq = json.loads(await future)
                    token = (_term, _seq)
                    current_sequence_numbers.remove(token)
                    waiting_sequence_numbers.append(token)

                    if current_sequence_numbers:
                        oldest_running = min(current_sequence_numbers)[1]
                    else:
                        oldest_running = last_seq_no

                    finished = [seq[1] for seq in waiting_sequence_numbers if seq[1] < oldest_running]
                    if finished:
                        new_completed = max(finished)
                        if new_completed != completed_seq_no:
                            completed_seq_no = new_completed
                            print("cursor head", completed_seq_no)

                recent_tokens = {key: value for key, value in recent_tokens.items() if value > (time.time() - 1000)}

                # if done:
                #     print("current active 1", len(futures))

            # When there are fewer than some large number of currently batched files, add more
            if len(current_sequence_numbers) < 10000:
                if last_seq_no < 0:
                    query = "*"
                else:
                    query = f"_seq_no: [{last_seq_no} TO *]"
                batch = al_client.search.file(query, sort="_seq_no asc", rows=BATCH_SIZE, fl=FL)

                futures_before = len(futures)
                for item in batch['items']:
                    # Get the current highest sequence number being processed
                    last_seq_no = max(item['_seq_no'], last_seq_no)

                    # Track all active sequence numbers, and launch a task
                    token = (item['_primary_term'], item['_seq_no'])
                    token_str = json.dumps(token)
                    if token_str in recent_tokens:
                        continue
                    recent_tokens[token_str] = time.time()
                    current_sequence_numbers.append(token)
                    try:
                        future = await house_client.ingest(item['sha256'], item['classification'],
                                                           item.get('expiry_ts', None), token=token_str)
                        futures.add(future)
                    except DuplicateToken:
                        pass

                successful_search = len(futures) > futures_before

                # if batch['items']:
                #     print("current active 2", len(futures))

            if not futures and not successful_search:
                print("Finished, waiting for new files")
                await asyncio.sleep(60)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        prog='ingest',
        description='Ingest files from assemblyline into hauntedhouse',
    )
    parser.add_argument("--trust-all", help="ignore server verification", action='store_true')
    args = parser.parse_args()

    asyncio.run(socket_main(verify=not args.trust_all))
