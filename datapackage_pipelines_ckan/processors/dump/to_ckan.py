import os
import json
import hashlib

from tabulator import Stream
import datapackage as datapackage_lib
from ckan_datapackage_tools import converter
from datapackage_pipelines.lib.dump.dumper_base import FileDumper, DumperBase
from datapackage_pipelines.utilities.resources import is_a_url

from tableschema_ckan_datastore import Storage

from datapackage_pipelines_ckan.utils import (
    make_ckan_request, get_ckan_error, get_env_param
)

import logging
log = logging.getLogger(__name__)


class CkanDumper(FileDumper):

    def initialize(self, parameters):
        super(CkanDumper, self).initialize(parameters)

        base_path = "/api/3/action"
        self.__base_url = get_env_param(parameters['ckan-host']).rstrip('/')
        self.__base_endpoint = self.__base_url + base_path

        self.__ckan_api_key = parameters.get('ckan-api-key')
        self.__ckan_log_resource = parameters.get('ckan-log-resource')
        self.__ckan_log = []
        self.__dataset_resources = []
        self.__dataset_id = None
        self.__dataset_ids = {}
        self.__push_to_datastore = \
            parameters.get('push_resources_to_datastore', False)
        self.__push_to_datastore_method = \
            parameters.get('push_resources_to_datastore_method', 'insert')
        if self.__push_to_datastore_method \
           not in ['insert', 'upsert', 'update']:
            raise RuntimeError(
                'push_resources_to_datastore_method must be one of '
                '\'insert\', \'upsert\' or \'update\'.')

    def prepare_datapackage(self, datapackage, params):
        datapackage = super(CkanDumper, self).prepare_datapackage(
            datapackage, params
        )
        if self.__ckan_log_resource:
            datapackage['resources'].append({
                'dpp:streaming': True,
                'name': self.__ckan_log_resource,
                'path': self.__ckan_log_resource + '.csv',
                'schema': {'fields': [
                    {'name': 'dataset_id', 'type': 'string'},
                    {'name': 'dataset_name', 'type': 'string'},
                    {'name': 'resource_id', 'type': 'string'},
                    {'name': 'resource_name', 'type': 'string'},
                    {'name': 'resource_url', 'type': 'string'},
                    {'name': 'resource_file', 'type': 'string'},
                    {'name': 'error', 'type': 'string'}]
                }
            })
        return datapackage

    def get_resource_dataset_id(self, resource):
        if self.__dataset_id:
            return self.__dataset_id
        elif resource.get('dataset-name'):
            return self.__dataset_ids.get(resource['dataset-name'])
        else:
            return None

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
                dataset_id = self.get_resource_dataset_id(resource)
                ckan_log_row = {'dataset_id': dataset_id,
                                'dataset_name': resource.get('dataset-name'),
                                'resource_name': resource.get('name')}
                if dataset_id:
                    resource_metadata = {
                        'package_id': dataset_id,
                        'name': resource.get('dataset-resource-name',
                                             resource['name']),
                    }
                    if 'format' in resource:
                        resource_metadata.update({
                            'format': resource['format']
                        })
                    streamed_from = resource['dpp:streamedFrom']
                    if is_a_url(streamed_from):
                        resource_metadata['url'] = streamed_from
                        request_params = {
                            'json': resource_metadata,
                        }
                        ckan_log_row.update(resource_file=None,
                                            resource_url=streamed_from)
                    else:
                        resource_metadata.update(url='url', url_type='upload')
                        request_params = {
                            'data': resource_metadata,
                            'files': {
                                'upload': (streamed_from,
                                           open(streamed_from, 'rb'))
                            }
                        }
                        ckan_log_row.update(resource_file=streamed_from,
                                            resource_url=None)
                    self._create_ckan_resource(request_params, ckan_log_row)
                else:
                    self.__ckan_log.append(dict(
                        ckan_log_row, resource_id=None, resource_url=None,
                        resource_file=None, error='no related dataset_id')
                    )

        # Handle each resource in resource_iterator
        for resource in resource_iterator:
            resource_spec = resource.spec
            ret = self.handle_resource(DumperBase.schema_validator(resource),
                                       resource_spec,
                                       parameters,
                                       datapackage)
            ret = self.row_counter(datapackage, resource_spec, ret)
            yield ret

        if self.__ckan_log_resource:
            yield (row for row in self.__ckan_log)

        stats['count_of_rows'] = DumperBase.get_attr(datapackage,
                                                     self.datapackage_rowcount)
        stats['bytes'] = DumperBase.get_attr(datapackage,
                                             self.datapackage_bytes)
        stats['hash'] = DumperBase.get_attr(datapackage, self.datapackage_hash)
        stats['dataset_name'] = datapackage['name']

    def handle_dataset(self, parameters, datapackage=None,
                       dataset_properties=None):
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

        if datapackage:
            dp = datapackage_lib.DataPackage(datapackage)
            dataset.update(converter.datapackage_to_dataset(dp))

            self.__dataset_resources = dataset.get('resources', [])
            if self.__dataset_resources:
                del dataset['resources']

        # Merge dataset-properties from parameters into dataset.
        dataset_props_from_params = parameters.get('dataset-properties')
        if dataset_props_from_params:
            dataset.update(dataset_props_from_params)

        if dataset_properties:
            dataset.update(dataset_properties)

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

        ckan_log_row = {'dataset_name': dataset['name'],
                        'resource_id': None, 'resource_name': None,
                        'resource_url': None, 'resource_file': None}

        if ckan_error or not response['success']:
            self.__ckan_log.append(dict(
                ckan_log_row, dataset_id=None, error=json.dumps(ckan_error))
            )
            if self.__ckan_log_resource:
                return None
            else:
                log.exception('CKAN returned an error: '
                              '' + json.dumps(ckan_error))
                raise Exception

        dataset_id = response['result']['id']
        self.__ckan_log.append(dict(
            ckan_log_row, dataset_id=dataset_id, error=None)
        )
        return dataset_id

    def handle_datapackage(self, datapackage, parameters, stats):
        '''Create or update ckan dataset/s from datapackage and parameters'''

        datasets = [descriptor['dataset-properties']
                    for descriptor in datapackage['resources']
                    if descriptor.get('dataset-properties',
                                      {}).get('name')]
        if len(datasets) > 0:
            for dataset in datasets:
                self.__dataset_ids[dataset['name']] = self.handle_dataset(
                    parameters, dataset_properties=dataset
                )
        else:
            self.__dataset_id = self.handle_dataset(parameters, datapackage)

    def rows_processor(self, resource, spec, temp_file,
                       writer, fields, datapackage):
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

        dataset_id = self.get_resource_dataset_id(spec)
        ckan_log_row = {'dataset_id': dataset_id,
                        'dataset_name': spec.get('dataset-name'),
                        'resource_name': spec.get('name')}
        if dataset_id:
            resource_metadata = {
                'package_id': dataset_id,
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
            ckan_log_row.update(resource_url=None, resource_file=ckan_filename)
            request_params = {
                'data': resource_metadata,
                'files': resource_files
            }
            try:
                # Create the CKAN resource
                create_result = self._create_ckan_resource(request_params,
                                                           ckan_log_row)
                if self.__push_to_datastore and create_result:
                    # Create the DataStore resource
                    storage = Storage(base_url=self.__base_url,
                                      dataset_id=dataset_id,
                                      api_key=self.__ckan_api_key)
                    resource_id = create_result['id']
                    storage.create(resource_id, spec['schema'])
                    storage.write(resource_id,
                                  Stream(temp_file.name, format='csv').open(),
                                  method=self.__push_to_datastore_method)
            except Exception as e:
                if self.__ckan_log_resource:
                    self.__ckan_log.append(
                        dict(ckan_log_row, resource_id=None, error=str(e))
                    )
                else:
                    raise e
            finally:
                os.unlink(filename)
        else:
            self.__ckan_log.append(dict(
                ckan_log_row, resource_id=None, resource_url=None,
                resource_file=None, error='no related dataset_id')
            )
            os.unlink(filename)

    def _create_ckan_resource(self, request_params, ckan_log_row):
        resource_create_url = '{}/resource_create'.format(self.__base_endpoint)

        create_response = make_ckan_request(resource_create_url,
                                            api_key=self.__ckan_api_key,
                                            method='POST',
                                            **request_params)

        ckan_error = get_ckan_error(create_response)
        if ckan_error:
            if self.__ckan_log_resource:
                self.__ckan_log.append(dict(ckan_log_row, resource_id=None,
                                            error=json.dumps(ckan_error)))
                return None
            else:
                log.exception('CKAN returned an error when creating '
                              'a resource: ' + json.dumps(ckan_error))
                raise Exception

        self.__ckan_log.append(dict(
            ckan_log_row, resource_id=create_response['result']['id'],
            error=None)
        )
        return create_response['result']


if __name__ == '__main__':
    CkanDumper()()
