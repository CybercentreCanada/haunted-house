from __future__ import annotations
import typing
import logging
import json
import ssl

import requests
from websockets.sync.client import connect


logger = logging.getLogger('hauntedhouse.client')


def to_websocket(url: str) -> str:
    """Switch an http url to websocket."""
    if url.lower().startswith("https://"):
        return 'wss://' + url[8:]
    return 'wss://' + url


class Client:
    def __init__(self, address: str, api_key: str, verify: typing.Optional[str]):
        self.address = address

        self.session = requests.Session()
        if verify:
            self.session.verify = verify
        else:
            self.session.verify = False
        self.session.headers['Authorization'] = 'Bearer ' + api_key

    def close(self):
        """Close requests session."""
        self.session.close()

    def start_search(self, yara_rule: str, rule_classification: str, search_classification: str,
                     creator: str, description: str, expiry, archive_only=False) -> str:
        """Start a new search."""
        logger.info("Start retrohunt search for %s", creator)
        indices = 'hot_and_archive'
        if archive_only:
            indices = 'archive'

        # Send the search request
        result = self.session.post(self.address + '/search/', json={
            # Classification for the retrohunt job
            'classification': rule_classification,
            # Maximum classification of results in the search
            'search_classification': search_classification,
            # User who created this retrohunt job
            'creator': creator,
            # Human readable description of this retrohunt job
            'description': description,
            # Expiry timestamp of this retrohunt job
            'expiry_ts': expiry,
            # What sorts of indices to run on
            'indices': indices,
            # Text of original yara signature run
            'yara_signature': yara_rule,
        })
        result.raise_for_status()
        response = result.json()
        return response['code']

    def repeat_search(self, key: str, search_classification: str, expiry):
        """Will trigger a search to rerun with the classification expanded to include the given value."""
        logger.info("Repeat retrohunt search %s", key)
        # Send the search request
        result = self.session.post(self.address + '/repeat/', json={
            'key': key,
            'search_classification': search_classification,
            'expiry': expiry,
        })
        result.raise_for_status()

    def search_status(self, code: str) -> typing.Generator[dict, None, None]:
        """Connect to the status websocket and stream the results."""
        # prepare connection info
        url = to_websocket(self.address) + '/search/' + code
        headers = {}
        for name, value in self.session.headers.items():
            if isinstance(value, bytes):
                headers[name] = value.decode()
            else:
                headers[name] = value

        # Setup SSL settings
        if isinstance(self.session.verify, str):
            # Create a context that expects a specific certificate
            ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
            ssl_context.load_verify_locations(self.session.verify)
        elif self.session.verify:
            # Create a basic secure context
            ssl_context = ssl.create_default_context()
        else:
            # Create a basic insecure context
            ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)

        # Connect to status feed
        with connect(url, additional_headers=headers, ssl_context=ssl_context) as ws:
            while True:
                message = json.loads(ws.recv())
                yield message
                if message.get('type', '') == 'finished':
                    break
