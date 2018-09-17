import json

from datapackage_pipelines.utilities.resources import (
    PATH_PLACEHOLDER, PROP_STREAMED_FROM
)
from datapackage_pipelines.generators import slugify
from datapackage_pipelines.wrapper import ingest, spew

from datapackage_pipelines_ckan.utils import (
    make_ckan_request, get_ckan_error, get_env_param
)

import logging
log = logging.getLogger(__name__)


class AddCkanResource(object):

    def get_parameters(self, parameters):
        self.parameters = parameters
        self.ckan_host = get_env_param(parameters.pop('ckan-host'))
        self.ckan_api_key = parameters.pop('ckan-api-key', None)
        self.resource_id = parameters.pop('resource-id')

    def get_resource_show_url(self):
        return '{ckan_host}/api/3/action/resource_show'.format(
            ckan_host=self.ckan_host)

    def get_ckan_resource(self, resource_show_url):
        response = make_ckan_request(resource_show_url,
                                     params=dict(id=self.resource_id),
                                     api_key=self.ckan_api_key)

        ckan_error = get_ckan_error(response)
        if ckan_error:
            if 'Not found: Resource ' \
               'was not found.' in ckan_error.get('message', []):
                log.exception('CKAN resource {} '
                              'was not found.'.format(self.resource_id))
            else:
                log.exception('CKAN returned an error: '
                              '' + json.dumps(ckan_error))

            raise Exception

        return response['result']

    def update_ckan_resource(self, resource):
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

        resource.update(self.parameters)

    def __call__(self, *args, **kwargs):
        parameters, datapackage, res_iter = ingest()
        self.get_parameters(parameters)
        resource_show_url = self.get_resource_show_url()
        resource = self.get_ckan_resource(resource_show_url)
        self.update_ckan_resource(resource)
        datapackage['resources'].append(resource)
        spew(datapackage, res_iter)


if __name__ == '__main__' or spew.__class__.__name__ == 'MagicMock':
    AddCkanResource()()
