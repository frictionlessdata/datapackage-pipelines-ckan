import json
import hashlib

import datapackage as datapackage_lib
from ckan_datapackage_tools import converter
from datapackage_pipelines.lib.dump.dumper_base import DumperBase

from datapackage_pipelines_ckan.utils import make_ckan_request

import logging
log = logging.getLogger(__name__)


class CkanDumper(DumperBase):

    def initialize(self, parameters):
        super(CkanDumper, self).initialize(parameters)
        self.ckan_base_url = '{ckan_host}/api/3/action/'.format(
            ckan_host=parameters['ckan-host'])
        self.ckan_api_key = parameters.get('ckan-api-key')
        self.dataset_resources = []

    def prepare_datapackage(self, datapackage, params):
        # Ensure datapackage has required data to be able to build a dataset
        return super(CkanDumper, self).prepare_datapackage(datapackage, params)

    def handle_datapackage(self, datapackage, parameters, stats):
        '''Create or update a ckan dataset from datapackage and parameters'''

        super(CkanDumper, self).handle_datapackage(datapackage,
                                                   parameters, stats)

        dp = datapackage_lib.DataPackage(datapackage)
        dataset = converter.datapackage_to_dataset(dp)

        self.dataset_resources = dataset.get('resources', [])
        if self.dataset_resources:
            del dataset['resources']

        # Merge dataset-properties from parameters into dataset.
        dataset_props_from_params = parameters.get('dataset-properties')
        if dataset_props_from_params:
            dataset.update(dataset_props_from_params)

        log.debug(dataset)

        package_create_url = \
            '{ckan_base_url}package_create'.format(
                ckan_base_url=self.ckan_base_url)

        response = make_ckan_request(package_create_url,
                                     method='POST',
                                     data=dataset,
                                     api_key=self.ckan_api_key)

        ckan_error = self._get_ckan_error(response)
        if ckan_error \
           and parameters.get('overwrite_existing') \
           and 'That URL is already in use.' in ckan_error.get('name', []):

            package_update_url = '{ckan_base_url}package_update'.format(
                ckan_base_url=self.ckan_base_url)

            log.info('CKAN dataset with url already exists. '
                     'Attempting package_update.')
            response = make_ckan_request(package_update_url,
                                         method='POST',
                                         data=dataset,
                                         api_key=self.ckan_api_key)
            ckan_error = self._get_ckan_error(response)

        if ckan_error:
            log.exception('CKAN returned an error: ' + json.dumps(ckan_error))
            raise Exception

    def handle_resources(self, datapackage,
                         resource_iterator,
                         parameters, stats):
        # Calculate datapackage hash
        if self.datapackage_hash:
            datapackage_hash = hashlib.md5(
                        json.dumps(datapackage,
                                   sort_keys=True,
                                   ensure_ascii=True).encode('ascii')
                    ).hexdigest()
            DumperBase.set_attr(datapackage, self.datapackage_hash,
                                datapackage_hash)

        # Handle the datapackage first!
        self.handle_datapackage(datapackage, parameters, stats)

        # Then handle each resource
        for resource in resource_iterator:
            resource_spec = resource.spec
            ret = self.handle_resource(DumperBase.schema_validator(resource),
                                       resource_spec,
                                       parameters,
                                       datapackage)
            ret = self.row_counter(datapackage, resource_spec, ret)
            yield ret

    def handle_resource(self, resource, spec, parameters, datapackage):
        pass

    def _get_ckan_error(self, response):
        '''Return the error from a ckan json response, or None.'''
        ckan_error = None
        if not response['success'] and response['error']:
            ckan_error = response['error']
        return ckan_error


if __name__ == '__main__':
    CkanDumper()()
