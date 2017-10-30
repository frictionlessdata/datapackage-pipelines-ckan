import os
import json
import hashlib

from tabulator import Stream
import datapackage as datapackage_lib
from ckan_datapackage_tools import converter
from datapackage_pipelines.lib.dump.dumper_base import FileDumper, DumperBase
from tableschema_ckan_datastore import Storage

from datapackage_pipelines_ckan.utils import make_ckan_request, get_ckan_error

import logging
log = logging.getLogger(__name__)


class CkanDumper(FileDumper):

    def initialize(self, parameters):
        super(CkanDumper, self).initialize(parameters)

        base_path = "/api/3/action"
        self.__base_url = parameters['ckan-host']
        self.__base_endpoint = self.__base_url + base_path

        self.__ckan_api_key = parameters.get('ckan-api-key')
        self.__dataset_resources = []
        self.__dataset_id = None
        self.__push_to_datastore = \
            parameters.get('push_resources_to_datastore', False)

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
                    'package_id': self.__dataset_id,
                    'url': resource['dpp:streamedFrom'],
                    'name': resource['name'],
                }
                if 'format' in resource:
                    resource_metadata.update({'format': resource['format']})
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

        # core dataset properties
        dataset = {
            'title': '',
            'version': '',
            'state': 'active',
            'url': '',
            'notes': '',
            'license_id': '',
            'author': '',
            'author_email': '',
            'maintainer': '',
            'maintainer_email': '',
            'owner_org': None,
            'private': False
        }

        dp = datapackage_lib.DataPackage(datapackage)
        dataset.update(converter.datapackage_to_dataset(dp))

        self.__dataset_resources = dataset.get('resources', [])
        if self.__dataset_resources:
            del dataset['resources']

        # Merge dataset-properties from parameters into dataset.
        dataset_props_from_params = parameters.get('dataset-properties')
        if dataset_props_from_params:
            dataset.update(dataset_props_from_params)

        package_create_url = '{}/package_create'.format(self.__base_endpoint)

        response = make_ckan_request(package_create_url,
                                     method='POST',
                                     json=dataset,
                                     api_key=self.__ckan_api_key)

        ckan_error = get_ckan_error(response)
        if ckan_error \
           and parameters.get('overwrite_existing') \
           and 'That URL is already in use.' in ckan_error.get('name', []):

            package_update_url = \
                '{}/package_update'.format(self.__base_endpoint)

            log.info('CKAN dataset with url already exists. '
                     'Attempting package_update.')
            response = make_ckan_request(package_update_url,
                                         method='POST',
                                         json=dataset,
                                         api_key=self.__ckan_api_key)
            ckan_error = get_ckan_error(response)

        if ckan_error:
            log.exception('CKAN returned an error: ' + json.dumps(ckan_error))
            raise Exception

        if response['success']:
            self.__dataset_id = response['result']['id']

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
            'package_id': self.__dataset_id,
            'url': 'url',
            'url_type': 'upload',
            'name': spec['name'],
            'hash': spec['hash']
        }
        if 'encoding' in spec:
            resource_metadata.update({'encoding': spec['encoding']})
        if 'format' in spec:
            resource_metadata.update({'format': spec['format']})
        ckan_filename = os.path.basename(spec['path'])
        resource_files = {
            'upload': (ckan_filename, open(temp_file.name, 'rb'))
        }
        request_params = {
            'data': resource_metadata,
            'files': resource_files
        }
        try:
            # Create the CKAN resource
            create_result = self._create_ckan_resource(request_params)
            if self.__push_to_datastore:
                # Create the DataStore resource
                storage = Storage(base_url=self.__base_url,
                                  dataset_id=self.__dataset_id,
                                  api_key=self.__ckan_api_key)
                resource_id = create_result['id']
                storage.create(resource_id, spec['schema'])
                storage.write(resource_id,
                              Stream(temp_file.name, format='csv').open(),
                              method='insert')
        except Exception as e:
            raise e
        finally:
            os.unlink(filename)

    def _create_ckan_resource(self, request_params):
        resource_create_url = '{}/resource_create'.format(self.__base_endpoint)

        create_response = make_ckan_request(resource_create_url,
                                            api_key=self.__ckan_api_key,
                                            method='POST',
                                            **request_params)

        ckan_error = get_ckan_error(create_response)
        if ckan_error:
            log.exception('CKAN returned an error when creating '
                          'a resource: ' + json.dumps(ckan_error))
            raise Exception
        return create_response['result']


if __name__ == '__main__':
    CkanDumper()()
