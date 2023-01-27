import asyncio
import json
import random
import time
from pprint import pprint

import arrow
import aiohttp
from assemblyline_client import get_client
from assemblyline.common.classification import Classification

import config

FL = 'classification,sha256,expiry_ts,_seq_no'


def token(item):
    return {'Token': item}


def prepare_classification(ce, classification):
    parts = ce.get_access_control_parts(classification)

    group1 = parts['__access_grp1__']
    group2 = parts['__access_grp2__']

    if '__EMPTY__' in group1:
        group1.remove('__EMPTY__')
    if '__EMPTY__' in group2:
        group2.remove('__EMPTY__')

    top = []

    top.append(token(ce._get_c12n_level_text(parts['__access_lvl__'])))

    for item in parts['__access_req__']:
        top.append(token(item))

    if group1:
        if len(group1) > 1:
            top.append({"Or": [token(g) for g in group1]})
        else:
            top.append(token(group1[0]))

    if group2:
        if len(group2) > 1:
            top.append({"Or": [token(g) for g in group2]})
        else:
            top.append(token(group2[0]))

    if len(top) == 0:
        return "Always"
    if len(top) == 1:
        return top[0]
    else:
        return {"And": top}


def pretty_classification(item):
    if isinstance(item, str):
        return item
    if 'And' in item:
        parts = [pretty_classification(x) for x in item["And"]]
        return 'And(' + ', '.join(parts) + ')'
    if 'Or' in item:
        parts = [pretty_classification(x) for x in item["Or"]]
        return 'Or(' + ', '.join(parts) + ')'
    if 'Token' in item:
        return item['Token']
    raise NotImplementedError()


async def ingest_call(session, ce, item):
    await asyncio.sleep(3 + random.random() * 3)
    print(item['classification'])
    print(ce.get_access_control_parts(item['classification']))


    # body = {
    #     'hash': item['sha256'],
    #     'access': ,
    #     'expiry': arrow.get(item['expiry_ts']).int_timestamp
    # }



    # async with session.post(config.HAUNTEDHOUSE_URL + "/ingest/sha256", body=body) as resp:
    #     if resp.status != 200:
    #         raise ValueError()

    return item['_seq_no']


async def main():
    # config = json.load(open('config.json'))

    client = get_client(config.ASSEMBLYLINE_URL, apikey=(config.ASSEMBLYLINE_USER, config.ASSEMBLYLINE_API_KEY))

    classification_definition = client._connection.get('api/v4/help/classification_definition')
    ce = Classification(classification_definition['original_definition'])
    assert ce.enforce
    print(ce.original_definition.keys())

    for item in ce.list_all_classification_combinations(long_format=True):
        print(item)
        # print('\t', ce.normalize_classification(item))
        print(pretty_classification(prepare_classification(ce, item)))
        print()

    return

    async with aiohttp.ClientSession(headers={'Authorization': 'Bearer ' + config.HAUNTEDHOUSE_KEY}) as session:

        completed_sequence_no = None
        next_sequence_no = None
        current_sequence_numbers = []
        waiting_sequence_numbers = []
        tasks = set()

        while True:

            if len(tasks) < 1000:
                if next_sequence_no is None:
                    batch = client.search.file("*", sort="_seq_no asc", rows=config.BATCH_SIZE, fl=FL)
                else:
                    batch = client.search.file(f"_seq_no: {{{next_sequence_no} TO *]", sort="_seq_no asc", rows=config.BATCH_SIZE, fl=FL)

                for item in batch['items']:
                    # Get the current highest sequence number being processed
                    if next_sequence_no is None:
                        next_sequence_no = item['_seq_no']
                    next_sequence_no = max(next_sequence_no, item['_seq_no'])

                    # Track all active sequence numbers, and launch a task
                    current_sequence_numbers.append(item['_seq_no'])
                    tasks.add(asyncio.create_task(ingest_call(session, ce, item)))

            if tasks:
                done, tasks = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
                for finished in done:
                    seq = await finished
                    current_sequence_numbers.remove(seq)
                    waiting_sequence_numbers.append(seq)

                    if current_sequence_numbers:
                        oldest_running = min(current_sequence_numbers)
                    else:
                        oldest_running = next_sequence_no
                    finished = [seq for seq in waiting_sequence_numbers if seq < oldest_running]
                    if finished:
                        completed_sequence_no = max(finished)
                        # print(completed_sequence_no, len(finished), len(tasks))
            else:
                print("Waiting for more")
                await asyncio.sleep(60)


if __name__ == '__main__':
    asyncio.run(main())
