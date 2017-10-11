import os
import json

import requests

from datapackage_pipelines.utilities.resources import (
    PATH_PLACEHOLDER, PROP_STREAMED_FROM
)
from datapackage_pipelines.generators import slugify
from datapackage_pipelines.wrapper import ingest, spew

import logging
log = logging.getLogger(__name__)

parameters, datapackage, res_iter = ingest()

ckan_host = parameters.pop('ckan-host')
ckan_api_key = parameters.pop('ckan-api-key', None)
request_headers = {}
if ckan_api_key:
    if ckan_api_key.startswith('env:'):
        ckan_api_key = os.environ.get(ckan_api_key[4:])
    request_headers.update({'Authorization': ckan_api_key})
resource_id = parameters.pop('resource-id')
resource_show_url = '{ckan_host}/api/3/action/resource_show'.format(
                    ckan_host=ckan_host)

response = requests.get(resource_show_url, params=dict(id=resource_id),
                        headers=request_headers)

try:
    resource = response.json()['result']
except json.decoder.JSONDecodeError:
    log.error('Expected JSON in response from: {}'.format(resource_show_url))
    raise

if 'name' in resource:
    if 'title' not in resource:
        resource['title'] = resource['name']
    resource['name'] = slugify(resource['name']).lower()

if 'format' in resource:
    resource['format'] = resource['format'].lower()

if 'url' in resource:
    resource['path'] = PATH_PLACEHOLDER
    resource[PROP_STREAMED_FROM] = resource['url']
    del resource['url']

del resource['hash']

resource.update(parameters)

datapackage['resources'].append(resource)

spew(datapackage, res_iter)
