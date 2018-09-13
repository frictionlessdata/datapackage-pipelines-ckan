import json

from datapackage_pipelines.utilities.resources import (
    PATH_PLACEHOLDER, PROP_STREAMED_FROM
)
from datapackage_pipelines.generators import slugify
from datapackage_pipelines.wrapper import ingest, spew

from datapackage_pipelines_ckan.utils import make_ckan_request, get_ckan_error, get_env_param

import logging
log = logging.getLogger(__name__)

parameters, datapackage, res_iter = ingest()

ckan_host = get_env_param(parameters.pop('ckan-host'))
ckan_api_key = parameters.pop('ckan-api-key', None)
resource_id = parameters.pop('resource-id')
resource_show_url = '{ckan_host}/api/3/action/resource_show'.format(
                    ckan_host=ckan_host)

response = make_ckan_request(resource_show_url,
                             params=dict(id=resource_id),
                             api_key=ckan_api_key)

ckan_error = get_ckan_error(response)
if ckan_error:
    if 'Not found: Resource was not found.' in ckan_error.get('message', []):
        log.exception('CKAN resource {} was not found.'.format(resource_id))
    else:
        log.exception('CKAN returned an error: ' + json.dumps(ckan_error))

    raise Exception

resource = response['result']

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
