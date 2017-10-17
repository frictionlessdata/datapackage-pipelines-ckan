import os
import json
import hashlib

import datapackage as datapackage_lib
from ckan_datapackage_tools import converter
from datapackage_pipelines.lib.dump.dumper_base import FileDumper, DumperBase

from datapackage_pipelines_ckan.utils import make_ckan_request

import logging
log = logging.getLogger(__name__)


class CkanDumper(FileDumper):

    def initialize(self, parameters):
        super(CkanDumper, self).initialize(parameters)
        self.ckan_base_url = '{ckan_host}/api/3/action/'.format(
            ckan_host=parameters['ckan-host'])
        self.ckan_api_key = parameters.get('ckan-api-key')
        self.dataset_resources = []
        self.dataset_id = None

    def handle_resources(self, datapackage,
                         resource_iterator,
                         parameters, stats):
        '''
            This method handles most of the control flow for creating
            dataset/resources on ckan.

            First we handle the dataset creation, then, if successful, we
            create each resource.
        '''

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

        # Handle non-streaming resources
        for resource in datapackage['resources']:
            if not resource.get('dpp:streaming', False):
                resource_metadata = {
                    'package_id': self.dataset_id,
                    'url': resource['dpp:streamedFrom'],
                    'name': resource['name'],
                }
                request_params = {
                    'json': resource_metadata
                }

                self._create_ckan_resource(request_params)

        # Handle each resource in resource_iterator
        for resource in resource_iterator:
            resource_spec = resource.spec
            ret = self.handle_resource(DumperBase.schema_validator(resource),
                                       resource_spec,
                                       parameters,
                                       datapackage)
            ret = self.row_counter(datapackage, resource_spec, ret)
            yield ret

        stats['count_of_rows'] = DumperBase.get_attr(datapackage,
                                                     self.datapackage_rowcount)
        stats['bytes'] = DumperBase.get_attr(datapackage,
                                             self.datapackage_bytes)
        stats['hash'] = DumperBase.get_attr(datapackage, self.datapackage_hash)
        stats['dataset_name'] = datapackage['name']

    def handle_datapackage(self, datapackage, parameters, stats):
        '''Create or update a ckan dataset from datapackage and parameters'''

        dp = datapackage_lib.DataPackage(datapackage)
        dataset = converter.datapackage_to_dataset(dp)

        self.dataset_resources = dataset.get('resources', [])
        if self.dataset_resources:
            del dataset['resources']

        # Merge dataset-properties from parameters into dataset.
        dataset_props_from_params = parameters.get('dataset-properties')
        if dataset_props_from_params:
            dataset.update(dataset_props_from_params)

        package_create_url = \
            '{ckan_base_url}package_create'.format(
                ckan_base_url=self.ckan_base_url)

        response = make_ckan_request(package_create_url,
                                     method='POST',
                                     json=dataset,
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
                                         json=dataset,
                                         api_key=self.ckan_api_key)
            ckan_error = self._get_ckan_error(response)

        if ckan_error:
            log.exception('CKAN returned an error: ' + json.dumps(ckan_error))
            raise Exception

        if response['success']:
            self.dataset_id = response['result']['id']

    def rows_processor(self, resource, spec, temp_file, writer, fields,
                       datapackage):
        file_formatter = self.file_formatters[spec['name']]
        for row in resource:
            file_formatter.write_row(writer, row, fields)
            yield row
        file_formatter.finalize_file(writer)

        # File size:
        filesize = temp_file.tell()
        DumperBase.inc_attr(datapackage, self.datapackage_bytes, filesize)
        DumperBase.inc_attr(spec, self.resource_bytes, filesize)

        # File Hash:
        if self.resource_hash:
            temp_file.seek(0)
            hasher = hashlib.md5()
            data = 'x'
            while len(data) > 0:
                data = temp_file.read(1024)
                hasher.update(data.encode('utf8'))
            DumperBase.set_attr(spec, self.resource_hash, hasher.hexdigest())

        # Finalise
        filename = temp_file.name
        temp_file.close()

        resource_metadata = {
            'package_id': self.dataset_id,
            'url': 'url',
            'url_type': 'upload',
            'name': spec['name'],
            'hash': spec['hash'],
            'encoding': spec['encoding']
        }
        ckan_filename = os.path.basename(spec['path'])
        resource_files = {
            'upload': (ckan_filename, open(temp_file.name, 'rb'))
        }
        request_params = {
            'data': resource_metadata,
            'files': resource_files
        }
        try:
            self._create_ckan_resource(request_params)
        except Exception as e:
            raise e
        finally:
            os.unlink(filename)

    def _create_ckan_resource(self, request_params):
        resource_create_url = '{ckan_base_url}resource_create'.format(
            ckan_base_url=self.ckan_base_url)

        create_response = make_ckan_request(resource_create_url,
                                            api_key=self.ckan_api_key,
                                            method='POST',
                                            **request_params)

        ckan_error = self._get_ckan_error(create_response)
        if ckan_error:
            log.exception('CKAN returned an error when creating '
                          'a resource: ' + json.dumps(ckan_error))
            raise Exception

    def _get_ckan_error(self, response):
        '''Return the error from a ckan json response, or None.'''
        ckan_error = None
        if not response['success'] and response['error']:
            ckan_error = response['error']
        return ckan_error


if __name__ == '__main__':
    CkanDumper()()
