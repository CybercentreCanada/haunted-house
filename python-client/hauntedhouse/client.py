from __future__ import annotations
import asyncio
import logging
import uuid
import json

import arrow
import aiohttp
import requests
import pydantic
from assemblyline.common.classification import Classification
from mquery_query_lib import yaraparse


logger = logging.getLogger('hauntedhouse.client')
FALSE_LEVELS = ['NULL', 'INV']


class IngestError(RuntimeError):
    ...


class DuplicateToken(KeyError):
    ...


class SearchStatus(pydantic.BaseModel):
    code: str
    group: str
    stage: str
    errors: list[str]
    suspect_files: int
    pending_files: int
    filtered_files: int
    group_hits: int
    group_misses: int
    hits: list[str]
    truncated: bool


class Client:
    def __init__(self, address: str, api_key: str, classification: dict, verify: bool = True):
        self.address = address
        self.access_engine = Classification(classification)

        self.ingest_connection_lock = asyncio.Lock()
        self.ingest_connection: None | aiohttp.ClientWebSocketResponse = None
        self.ingest_task: None | asyncio.Task = None
        self.ingest_futures: dict[str, asyncio.Future] = {}

        conn = aiohttp.TCPConnector(limit=10, verify_ssl=verify)
        self.session = aiohttp.ClientSession(
            base_url=self.address,
            headers={'Authorization': 'Bearer ' + api_key},
            connector=conn
        )

        self.sync_session = requests.Session()
        self.sync_session.verify = verify
        self.sync_session.headers['Authorization'] = 'Bearer ' + api_key

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.close()

    async def close(self):
        await self.session.close()

    async def start_search(self, yara_rule: str, access_control: str, group: str = '',
                           archive_only=False) -> SearchStatus:
        # Parse the access string into a set of key words
        access_fields = self.prepare_access(access_control)
        search_view = self.prepare_classification(access_control)

        # If we only want archived material set the start date to the far future
        start_date = None
        if archive_only:
            start_date = '9000-01-01T00:00:00.000'

        # Send the search request
        result = await self.session.post('/search/', json={
            'view': search_view,
            'access': access_fields,
            'query': query_from_yara(yara_rule),
            'group': group,
            'yara_signature': yara_rule,
            'start_date': start_date,
            'end_date': None,
        })
        result.raise_for_status()

        # Parse the message
        return SearchStatus(**await result.json())

    def start_search_sync(self, yara_rule: str, access_control: str, group: str = '',
                          archive_only=False) -> SearchStatus:
        # Parse the access string into a set of key words
        access_fields = self.prepare_access(access_control)
        search_view = self.prepare_classification(access_control)

        # If we only want archived material set the start date to the far future
        start_date = None
        if archive_only:
            start_date = '9000-01-01T00:00:00.000'

        # Send the search request
        result = self.sync_session.post(self.address + '/search/', json={
            'view': search_view,
            'access': access_fields,
            'query': query_from_yara(yara_rule),
            'group': group,
            'yara_signature': yara_rule,
            'start_date': start_date,
            'end_date': None,
        })
        result.raise_for_status()

        # Parse the message
        return SearchStatus(**result.json())

    async def search_status(self, code: str, access: str) -> SearchStatus:
        access_parts = self.prepare_access(access)

        # Send the request
        result = await self.session.get('/search/' + code, json={'access': access_parts})
        result.raise_for_status()

        # Parse the message
        return SearchStatus(**await result.json())

    def search_status_sync(self, code: str, access: str) -> SearchStatus:
        access_parts = self.prepare_access(access)

        # Send the request
        result = self.sync_session.get(self.address + '/search/' + code, json={'access': access_parts})
        result.raise_for_status()

        # Parse the message
        return SearchStatus(**result.json())

    def prepare_access(self, access: str) -> list[str]:
        if not access:
            return []
        parts = self.access_engine.get_access_control_parts(access)
        terms = []
        terms.extend([
            key for key in self.access_engine.levels_map_stl.keys()
            if key not in FALSE_LEVELS and self.access_engine.levels_map[key] <= parts['__access_lvl__']
        ])
        terms.extend(parts['__access_req__'])
        terms.extend(parts['__access_grp1__'])
        terms.extend(parts['__access_grp2__'])
        while '__EMPTY__' in terms:
            terms.remove('__EMPTY__')
        return terms

    @staticmethod
    def _token(item):
        return {'Token': item}

    def prepare_classification(self, classification: str):
        parts = self.access_engine.get_access_control_parts(classification)

        group1 = parts['__access_grp1__']
        group2 = parts['__access_grp2__']

        if '__EMPTY__' in group1:
            group1.remove('__EMPTY__')
        if '__EMPTY__' in group2:
            group2.remove('__EMPTY__')

        top = []

        top.append(self._token(self.access_engine.levels_map[str(parts['__access_lvl__'])]))

        for item in parts['__access_req__']:
            top.append(self._token(item))

        if group1:
            if len(group1) > 1:
                top.append({"Or": [self._token(g) for g in group1]})
            else:
                top.append(self._token(group1[0]))

        if group2:
            if len(group2) > 1:
                top.append({"Or": [self._token(g) for g in group2]})
            else:
                top.append(self._token(group2[0]))

        if len(top) == 0:
            return "Always"
        if len(top) == 1:
            return top[0]
        else:
            return {"And": top}

    async def ingest(self, sha256: str, classification: str, expiry, token: None | str = None):
        token = str(token or uuid.uuid4().hex)
        if token in self.ingest_futures:
            raise DuplicateToken(token)

        future: asyncio.Future[str] = asyncio.Future()
        try:
            self.ingest_futures[token] = future
            if expiry is not None:
                expiry = arrow.get(expiry).int_timestamp
            ws = await self._get_ingest_socket()

            body = {
                'token': token,
                'hash': sha256,
                'access': self.prepare_classification(classification),
                'expiry': expiry,
                'block': True,
            }
            await ws.send_json(body)
            return future

        except Exception:
            self.ingest_futures.pop(token, None)
            raise

    async def _get_ingest_socket(self) -> aiohttp.ClientWebSocketResponse:
        async with self.ingest_connection_lock:
            if self.ingest_connection is None:
                self.ingest_connection = await self.session.ws_connect("/ingest/stream/", protocols=['ingest-stream'])
                self.ingest_task = asyncio.create_task(self._socket_listener(self.ingest_connection))
            return self.ingest_connection

    async def _socket_listener(self, ws: aiohttp.ClientWebSocketResponse):
        while not ws.closed:
            try:
                message = await ws.receive_json()
                # Figure out any token with the message
                token = message.get('token', None)
                if token is None:
                    logger.error(f"Unknown message from ingest feed: {message}")
                    continue

                # Find the future associated with the token
                future = self.ingest_futures.pop(token, None)
                if future is None:
                    logger.error(f"Unexpected message from ingest feed: {message}")
                    continue

                # Satisfy the future
                if message.get('success'):
                    future.set_result(message.get("token", ""))
                else:
                    future.set_exception(IngestError(message.get('error', '')))

            except Exception:
                logger.exception("Error in receiveed message")
        self.ingest_connection = None


def query_from_yara(yara_rule: str) -> str:
    rules = yaraparse.parse_yara(yara_rule)

    if len(rules) == 0:
        raise ValueError("A yara rule couldn't be found")
    elif len(rules) == 1:
        return rules[0].parse().query
    else:
        raise ValueError("Only a single yara rule expected")

