import argparse
import json
import asyncio
import os
import logging
import sys
import time

from assemblyline_client import get_client
import pydantic

from .client import Client, DuplicateToken


logger = logging.getLogger('hauntedhouse.ingest')
FL = 'classification,sha256,expiry_ts,_seq_no,_primary_term'


def get_env_data_as_dict(dotenv_path):
    result = {}
    with open(dotenv_path) as file_obj:
        lines = file_obj.read().splitlines()  # Removes \n from lines

    for line in lines:
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        if "#" in line:
            line = line.split("#")[0].strip()
        key, value = line.split("=", maxsplit=1)
        result[key] = value
    return result


class Config(pydantic.BaseModel):
    assemblyline_url: str
    assemblyline_user: str
    assemblyline_api_key: str
    hauntedhouse_url: str
    hauntedhouse_api_key: str
    write_path: str
    batch_size: int = pydantic.Field(default=1000)
    allow_disabled_access: bool = pydantic.Field(default=False)
    trust_all: bool = pydantic.Field(default=False)


async def socket_main(config: Config, verify: bool, count: int) -> None:
    logger.info("Connect to assemblyline for classification configuration")
    al_client = get_client(config.assemblyline_url, apikey=(config.assemblyline_user, config.assemblyline_api_key), verify=verify)
    classification_definition = al_client._connection.get('api/v4/help/classification_definition')

    try:
        os.makedirs(config.write_path)
    except FileExistsError:
        pass

    try:
        with open(os.path.join(config.write_path, "state.json"), 'r') as handle:
            data = json.load(handle)
            completed_seq_no = data['completed']
            last_seq_no = data['completed']
    except FileNotFoundError:
        last_seq_no = -1
        completed_seq_no = -1

    successful_search = False

    recent_tokens: dict[str, float] = {}
    current_sequence_numbers: list[tuple[int, int]] = []
    waiting_sequence_numbers: list[tuple[int, int]] = []
    futures: set[asyncio.Future[str]] = set()

    logger.info("Connect to hauntedhouse")
    async with Client(config.hauntedhouse_url, config.hauntedhouse_api_key,
                      classification_definition['original_definition'], verify=verify) as house_client:
        if not config.allow_disabled_access:
            assert house_client.access_engine.enforce

        logger.info("Starting loop")
        assignment = {}

        while True:
            await asyncio.sleep(0)

            # Process any completed ingestion, moving the cursor for completed sequence numbers ahead
            if futures:
                if not successful_search:
                    done, futures = await asyncio.wait(futures, timeout=30, return_when=asyncio.FIRST_COMPLETED)
                else:
                    done = set([f for f in futures if f.done()])
                    futures = futures - done

                if not done:
                    time.sleep(0.1)

                for future in done:
                    args = assignment.pop(future)
                    try:
                        _term, _seq = json.loads(await future)
                    except Exception:
                        try:
                            print("Retrying...")
                            future = await house_client.ingest(args[0], args[1], args[2], token=args[3])
                            assignment[future] = args
                            futures.add(future)
                        except DuplicateToken:
                            pass
                        continue

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
                            logger.info(f"cursor head {completed_seq_no}")
                            with open(os.path.join(config.write_path, "state.json"), 'w') as handle:
                                json.dump({
                                    'completed': completed_seq_no
                                }, handle)

                recent_tokens = {key: value for key, value in recent_tokens.items() if value > (time.time() - 1000)}

                # if done:
                #     logger.info("current active 1", len(futures))

            # When there are fewer than some large number of currently batched files, add more
            if len(current_sequence_numbers) < count:
                if last_seq_no < 0:
                    query = "*"
                else:
                    query = f"_seq_no: [{last_seq_no} TO *]"
                batch = al_client.search.file(query, sort="_seq_no asc", rows=config.batch_size, fl=FL)

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
                        assignment[future] = (item['sha256'], item['classification'], item.get('expiry_ts', None), token_str)
                        futures.add(future)
                    except DuplicateToken:
                        pass

                successful_search = len(futures) > futures_before

                # if batch['items']:
                #     logger.info("current active 2", len(futures))

            if not futures and not successful_search:
                logger.info("Finished, waiting for new files")
                await asyncio.sleep(60)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        prog='ingest',
        description='Ingest files from assemblyline into hauntedhouse',
    )
    parser.add_argument("--trust-all", help="ignore server verification", action='store_true')
    parser.add_argument("--count", help="number of files to have outstanding", default=10000)
    parser.add_argument("config", help="path to config file")
    args = parser.parse_args()
    count = int(args.count)

    if args.config.endswith(".json"):
        config = Config(**json.load(open(args.config)))
    else:
        config = Config(**get_env_data_as_dict(args.config))

    logger = logging.getLogger('hauntedhouse')
    logger.setLevel(logging.INFO)
    logger.addHandler(logging.StreamHandler(sys.stdout))

    if config.trust_all is True:
        trust_all = True
    else:
        trust_all = args.trust_all

    asyncio.run(socket_main(config, verify=not trust_all, count=count))
