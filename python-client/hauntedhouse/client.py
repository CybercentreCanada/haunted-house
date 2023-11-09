from __future__ import annotations
import asyncio
import logging
import uuid
# import typing

import arrow
import aiohttp
import requests
import pydantic


logger = logging.getLogger('hauntedhouse.client')
FALSE_LEVELS = ['NULL', 'INV']


class IngestError(RuntimeError):
    ...


class CouldNotParseRule(ValueError):
    ...


class DuplicateToken(KeyError):
    ...


class SearchStatus(pydantic.BaseModel):
    code: str
    finished: bool
    warnings: list[str]
    errors: list[str]
    hits: list[str]
    truncated: bool
    phase: str
    progress: tuple[int, int]


class Client:
    def __init__(self, address: str, api_key: str, verify: bool = True):
        self.address = address
        # self.access_engine = Classification(classification)

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

    async def start_search(self, yara_rule: str, access_control: str, archive_only=False) -> SearchStatus:

        # If we only want archived material set the start date to the far future
        start_date = None
        if archive_only:
            start_date = '9000-01-01T00:00:00.000'

        # Send the search request
        result = await self.session.post('/search/', json={
            'view': access_control,
            'classification': access_control,
            'yara_signature': yara_rule,
            'start_date': start_date,
            'end_date': None,
        })
        result.raise_for_status()

        # Parse the message
        return SearchStatus(**await result.json())

    def start_search_sync(self, yara_rule: str, access_control: str, archive_only=False) -> SearchStatus:

        # If we only want archived material set the start date to the far future
        start_date = None
        if archive_only:
            start_date = '9000-01-01T00:00:00.000'

        # Send the search request
        result = self.sync_session.post(self.address + '/search/', json={
            'view': access_control,
            'classification': access_control,
            'yara_signature': yara_rule,
            'start_date': start_date,
            'end_date': None,
        })
        result.raise_for_status()

        # Parse the message
        return SearchStatus(**result.json())

    async def search_status(self, code: str, access: str) -> SearchStatus:
        # Send the request
        result = await self.session.get('/search/' + code, json={'access': access})
        result.raise_for_status()

        # Parse the message
        return SearchStatus(**await result.json())

    def search_status_sync(self, code: str, access: str) -> SearchStatus:
        # Send the request
        result = self.sync_session.get(self.address + '/search/' + code, json={'access': access})
        result.raise_for_status()

        # Parse the message
        return SearchStatus(**result.json())

    async def ingest(self, sha256: str, classification: str, expiry, token: None | str = None):
        token = str(token or uuid.uuid4().hex)
        if token in self.ingest_futures:
            raise DuplicateToken(token)

        future: asyncio.Future[str] = asyncio.Future()
        try:
            if expiry is not None:
                expiry = arrow.get(expiry).int_timestamp
            ws = await self._get_ingest_socket()
            self.ingest_futures[token] = future

            body = {
                'token': token,
                'hash': sha256,
                'access': classification,
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
                self.ingest_futures = {}
            return self.ingest_connection

    async def _socket_listener(self, ws: aiohttp.ClientWebSocketResponse):
        while not ws.closed:
            try:
                message = await ws.receive_json()
                # Figure out any token with the message
                token = message.get('token', None)
                if token is None:
                    logger.error("Unknown message from ingest feed: %s", message)
                    continue

                # Find the future associated with the token
                future = self.ingest_futures.pop(token, None)
                if future is None:
                    logger.error("Unexpected message from ingest feed: %s", message)
                    continue

                # Satisfy the future
                if message.get('success'):
                    future.set_result(message.get("token", ""))
                else:
                    future.set_exception(IngestError(message.get('error', '')))
            except Exception:
                logger.exception("Error in receiveed message")
        self.ingest_connection = None

        # Clear futures wating on this socket to complete
        for future in self.ingest_futures.values():
            future.cancel()
