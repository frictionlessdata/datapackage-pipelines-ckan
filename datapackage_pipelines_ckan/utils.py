import os
import json
import requests

import logging
log = logging.getLogger(__name__)


def make_ckan_request(url, method='GET', params=None, data=None,
                      headers=None, api_key=None):
    '''Make a CKAN API request to `url` and return the json response'''

    if headers is None:
        headers = {}

    if api_key:
        if api_key.startswith('env:'):
            api_key = os.environ.get(api_key[4:])
        headers.update({'Authorization': api_key})

    response = requests.request(method, url, params=params, json=data,
                                headers=headers, allow_redirects=True)

    try:
        return response.json()
    except json.decoder.JSONDecodeError:
        log.error('Expected JSON in response from: {}'.format(url))
        raise
