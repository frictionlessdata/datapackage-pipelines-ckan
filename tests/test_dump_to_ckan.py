import importlib
import io
import json
import os
import unittest

import requests_mock
import mock

from datapackage_pipelines.wrapper.input_processor import ResourceIterator

import datapackage_pipelines_ckan.processors

import logging
log = logging.getLogger(__name__)


@mock.patch('datapackage_pipelines.lib.dump.dumper_base.ingest')
@mock.patch('datapackage_pipelines.lib.dump.dumper_base.spew')
def mock_dump_test(processor, ingest_tuple, mock_spew, mock_ingest):
    '''Helper function returns the `spew` for a given processor with a given
    `ingest` tuple.'''

    # Mock all calls to `ingest` to return `ingest_tuple`
    mock_ingest.return_value = ingest_tuple

    # Call processor
    file_path = processor
    module_name = '__main__'
    spec = importlib.util.spec_from_file_location(module_name, file_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    # Our processor called `spew`. Return the args it was called with.
    return mock_spew.call_args


class TestDumpToCkanProcessor(unittest.TestCase):

    @requests_mock.mock()
    def test_dump_to_ckan_no_resources(self, mock_request):

        base_url = 'https://demo.ckan.org/api/3/action/'
        package_create_url = '{}package_create'.format(base_url)

        mock_request.post(package_create_url,
                          json={
                            'success': True,
                            'result': {'id': 'ckan-package-id'}})

        # input arguments used by our mock `ingest`
        datapackage = {
            'name': 'my-datapackage',
            'project': 'my-project',
            'resources': []
        }
        params = {
            'ckan-host': 'https://demo.ckan.org',
            'ckan-api-key': 'my-api-key',
            'dataset-properties': {
                'extra_prop': 'hi'
            }
        }

        # Path to the processor we want to test
        processor_dir = \
            os.path.dirname(datapackage_pipelines_ckan.processors.__file__)
        processor_path = os.path.join(processor_dir, 'dump/to_ckan.py')

        # Trigger the processor with our mock `ingest` and capture what it will
        # returned to `spew`.
        spew_args, _ = mock_dump_test(processor_path,
                                      (params, datapackage, []))

        spew_res_iter = spew_args[1]
        assert list(spew_res_iter) == []

        requests = mock_request.request_history
        assert len(requests) == 1
        assert requests[0].url == package_create_url

    @requests_mock.mock()
    def test_dump_to_ckan_package_create_error(self, mock_request):
        '''Create failed due to existing package, no overwrite so raise
        exception'''

        base_url = 'https://demo.ckan.org/api/3/action/'
        package_create_url = '{}package_create'.format(base_url)

        mock_request.post(package_create_url,
                          json={
                            'success': False,
                            'error': {"__type": "Validation Error",
                                      "name": ["That URL is already in use."]}
                            })

        # input arguments used by our mock `ingest`
        datapackage = {
            'name': 'my-datapackage',
            'project': 'my-project',
            'resources': []
        }
        params = {
            'ckan-host': 'https://demo.ckan.org',
            'ckan-api-key': 'my-api-key'
        }

        # Path to the processor we want to test
        processor_dir = \
            os.path.dirname(datapackage_pipelines_ckan.processors.__file__)
        processor_path = os.path.join(processor_dir, 'dump/to_ckan.py')

        # Trigger the processor with our mock `ingest` and capture what it will
        # returned to `spew`.
        spew_args, _ = mock_dump_test(processor_path,
                                      (params, datapackage, []))

        spew_res_iter = spew_args[1]
        with self.assertRaises(Exception):
            list(spew_res_iter)

        requests = mock_request.request_history
        assert len(requests) == 1
        assert requests[0].url == package_create_url

    @requests_mock.mock()
    def test_dump_to_ckan_package_create_error_overwrite(self, mock_request):
        '''Create failed due to existing package, overwrite so update existing
        package.'''

        base_url = 'https://demo.ckan.org/api/3/action/'
        package_create_url = '{}package_create'.format(base_url)
        package_update_url = '{}package_update'.format(base_url)

        mock_request.post(package_create_url,
                          json={
                            'success': False,
                            'error': {"__type": "Validation Error",
                                      "name": ["That URL is already in use."]}
                            })
        mock_request.post(package_update_url,
                          json={
                            'success': True,
                            'result': {'id': 'ckan-package-id'}})

        # input arguments used by our mock `ingest`
        datapackage = {
            'name': 'my-datapackage',
            'project': 'my-project',
            'resources': []
        }
        params = {
            'ckan-host': 'https://demo.ckan.org',
            'ckan-api-key': 'my-api-key',
            'overwrite_existing': True
        }

        # Path to the processor we want to test
        processor_dir = \
            os.path.dirname(datapackage_pipelines_ckan.processors.__file__)
        processor_path = os.path.join(processor_dir, 'dump/to_ckan.py')

        # Trigger the processor with our mock `ingest` and capture what it will
        # returned to `spew`.
        spew_args, _ = mock_dump_test(processor_path,
                                      (params, datapackage, []))

        spew_res_iter = spew_args[1]
        assert list(spew_res_iter) == []

        requests = mock_request.request_history
        assert len(requests) == 2
        assert requests[0].url == package_create_url
        assert requests[1].url == package_update_url

    @requests_mock.mock()
    def test_dump_to_ckan_package_create_resources(self, mock_request):
        '''Create package with non-streaming resources.'''

        base_url = 'https://demo.ckan.org/api/3/action/'
        package_create_url = '{}package_create'.format(base_url)
        resource_create_url = '{}resource_create'.format(base_url)

        mock_request.post(package_create_url,
                          json={
                            'success': True,
                            'result': {'id': 'ckan-package-id'}})
        mock_request.post(resource_create_url,
                          json={
                            'success': True,
                            'result': {'id': 'ckan-resource-id'}})

        # input arguments used by our mock `ingest`
        datapackage = {
            'name': 'my-datapackage',
            'project': 'my-project',
            'resources': [{
                "dpp:streamedFrom": "https://example.com/file.csv",
                "name": "resource_not_streamed",
                "path": ".",
                "format": "csv"
            }, {
                "dpp:streamedFrom": "https://example.com/file_02.csv",
                "name": "resource_not_streamed_02",
                "path": "."
            }]
        }
        params = {
            'ckan-host': 'https://demo.ckan.org',
            'ckan-api-key': 'my-api-key',
            'overwrite_existing': True
        }

        # Path to the processor we want to test
        processor_dir = \
            os.path.dirname(datapackage_pipelines_ckan.processors.__file__)
        processor_path = os.path.join(processor_dir, 'dump/to_ckan.py')

        # Trigger the processor with our mock `ingest` and capture what it will
        # returned to `spew`.
        spew_args, _ = mock_dump_test(processor_path,
                                      (params, datapackage, []))

        spew_res_iter = spew_args[1]
        assert list(spew_res_iter) == []

        requests = mock_request.request_history
        assert len(requests) == 3
        assert requests[0].url == package_create_url
        assert requests[1].url == resource_create_url
        assert requests[2].url == resource_create_url

    @requests_mock.mock()
    def test_dump_to_ckan_package_create_streaming_resource(self,
                                                            mock_request):
        '''Create package with streaming resource.'''

        base_url = 'https://demo.ckan.org/api/3/action/'
        package_create_url = '{}package_create'.format(base_url)
        resource_create_url = '{}resource_create'.format(base_url)

        mock_request.post(package_create_url,
                          json={
                            'success': True,
                            'result': {'id': 'ckan-package-id'}})
        mock_request.post(resource_create_url,
                          json={
                            'success': True,
                            'result': {'id': 'ckan-resource-id'}})

        # input arguments used by our mock `ingest`
        datapackage = {
            'name': 'my-datapackage',
            'project': 'my-project',
            'resources': [{
                "dpp:streamedFrom": "https://example.com/file.csv",
                "dpp:streaming": True,
                "name": "resource_streamed.csv",
                "path": "data/file.csv",
                'schema': {'fields': [
                    {'name': 'first', 'type': 'string'},
                    {'name': 'last', 'type': 'string'}
                ]}
            }, {
                "dpp:streamedFrom": "https://example.com/file_02.csv",
                "name": "resource_not_streamed.csv",
                "path": "."
            }]
        }
        params = {
            'ckan-host': 'https://demo.ckan.org',
            'ckan-api-key': 'my-api-key',
            'overwrite_existing': True,
            'force-format': True
        }

        # Path to the processor we want to test
        processor_dir = \
            os.path.dirname(datapackage_pipelines_ckan.processors.__file__)
        processor_path = os.path.join(processor_dir, 'dump/to_ckan.py')

        # Trigger the processor with our mock `ingest` and capture what it will
        # returned to `spew`.
        json_file = {'first': 'Fred', 'last': 'Smith'}
        json_file = json.dumps(json_file)
        spew_args, _ = mock_dump_test(
            processor_path,
            (params, datapackage,
             iter([ResourceIterator(io.StringIO(json_file),
                                    datapackage['resources'][0],
                                    {'schema': {'fields': []}})
                   ])))

        spew_res_iter = spew_args[1]
        for r in spew_res_iter:
            list(r)  # iterate the row to yield it

        requests = mock_request.request_history
        assert len(requests) == 3
        assert requests[0].url == package_create_url
        assert requests[1].url == resource_create_url
        assert requests[2].url == resource_create_url

    @requests_mock.mock()
    def test_dump_to_ckan_package_create_streaming_resource_fail(self,
                                                                 mock_request):
        '''Create package with streaming resource, which failed to create
        resource.'''

        base_url = 'https://demo.ckan.org/api/3/action/'
        package_create_url = '{}package_create'.format(base_url)
        resource_create_url = '{}resource_create'.format(base_url)

        mock_request.post(package_create_url,
                          json={
                            'success': True,
                            'result': {'id': 'ckan-package-id'}})
        mock_request.post(resource_create_url,
                          json={
                            'success': False,
                            'error': {"__type": "Validation Error",
                                      "name": ["Some validation error."]}
                            })

        # input arguments used by our mock `ingest`
        datapackage = {
            'name': 'my-datapackage',
            'project': 'my-project',
            'resources': [{
                "dpp:streamedFrom": "https://example.com/file.csv",
                "dpp:streaming": True,
                "name": "resource_streamed.csv",
                "path": "data/file.csv",
                'schema': {'fields': [
                    {'name': 'first', 'type': 'string'},
                    {'name': 'last', 'type': 'string'}
                ]}
            }, {
                "dpp:streamedFrom": "https://example.com/file_02.csv",
                "name": "resource_not_streamed.csv",
                "path": "."
            }]
        }
        params = {
            'ckan-host': 'https://demo.ckan.org',
            'ckan-api-key': 'my-api-key',
            'overwrite_existing': True,
            'force-format': True
        }

        # Path to the processor we want to test
        processor_dir = \
            os.path.dirname(datapackage_pipelines_ckan.processors.__file__)
        processor_path = os.path.join(processor_dir, 'dump/to_ckan.py')

        # Trigger the processor with our mock `ingest` and capture what it will
        # returned to `spew`.
        json_file = {'first': 'Fred', 'last': 'Smith'}
        json_file = json.dumps(json_file)
        spew_args, _ = mock_dump_test(
            processor_path,
            (params, datapackage,
             iter([ResourceIterator(io.StringIO(json_file),
                                    datapackage['resources'][0],
                                    {'schema': {'fields': []}})
                   ])))

        spew_res_iter = spew_args[1]
        with self.assertRaises(Exception):
            for r in spew_res_iter:
                list(r)  # iterate the row to yield it

    @requests_mock.mock()
    def test_dump_to_ckan_package_create_streaming_resource_datastore(self, mock_request):  # noqa
        '''Create package with streaming resource, and pushing to datastore.'''

        package_id = 'ckan-package-id'
        base_url = 'https://demo.ckan.org/api/3/action/'
        package_create_url = '{}package_create'.format(base_url)
        resource_create_url = '{}resource_create'.format(base_url)
        package_show_url = '{}package_show?id={}'.format(base_url, package_id)
        datastore_search_url = \
            '{}datastore_search?resource_id=_table_metadata'.format(base_url)
        datastore_create_url = '{}datastore_create'.format(base_url)
        datastore_upsert_url = '{}datastore_upsert'.format(base_url)

        mock_request.post(package_create_url,
                          json={
                            'success': True,
                            'result': {'id': package_id}})
        mock_request.post(resource_create_url,
                          json={
                            'success': True,
                            'result': {'id': 'ckan-resource-id'}})
        mock_request.get(package_show_url,
                         json={
                            'success': True,
                            'result': {
                                'id': '7766839b-face-4336-8e1a-3c51c5e7634d',
                                'resources': [
                                    {
                                        'name': 'co2-mm-mlo_csv_not_streamed',
                                        'format': 'CSV',
                                        'url': 'https://pkgstore.datahub.io/core/co2-ppm:co2-mm-mlo_csv/data/co2-mm-mlo_csv.csv',
                                        'datastore_active': False,
                                        'cache_last_updated': None,
                                        'package_id': '7766839b-face-4336-8e1a-3c51c5e7634d',
                                        'id': '329e4271-8cc3-48c9-a219-c8eab52acc65',
                                    }, {
                                        'name': 'co2-mm-mlo_csv_streamed',
                                        'encoding': 'utf-8',
                                        'url': 'https://demo.ckan.org/dataset/7766839b-face-4336-8e1a-3c51c5e7634d/resource/723380d7-688a-465f-b0bd-ff6d1ec25680/download/co2-mm-mlo_csv_streamed.csv',
                                        'datastore_active': False,
                                        'format': 'CSV',
                                        'package_id': '7766839b-face-4336-8e1a-3c51c5e7634d',
                                        'id': '723380d7-688a-465f-b0bd-ff6d1ec25680',
                                    }
                                ],
                                'num_resources': 2,
                                'name': 'test-dataset-010203',
                                'title': 'Test Dataset'
                            }
                         })

        mock_request.get(datastore_search_url,
                         json={
                            'success': True,
                            'result': {
                                'resource_id': '_table_metadata',
                                'records': []
                            }})
        mock_request.post(datastore_create_url,
                          json={
                            'success': True,
                            'result': {
                                'resource_id': '7564690e-86ec-44de-a3f5-2cff9cbb521f'
                            }
                            })
        mock_request.post(datastore_upsert_url,
                          json={
                            'success': True
                          })
        # input arguments used by our mock `ingest`
        datapackage = {
            'name': 'my-datapackage',
            'project': 'my-project',
            'resources': [{
                "dpp:streamedFrom": "https://example.com/file.csv",
                "dpp:streaming": True,
                "name": "resource_streamed.csv",
                "path": "data/file.csv",
                'schema': {'fields': [
                    {'name': 'first', 'type': 'string'},
                    {'name': 'last', 'type': 'string'}
                ]}
            }, {
                "dpp:streamedFrom": "https://example.com/file_02.csv",
                "name": "resource_not_streamed.csv",
                "path": "."
            }]
        }
        params = {
            'ckan-host': 'https://demo.ckan.org',
            'ckan-api-key': 'my-api-key',
            'overwrite_existing': True,
            'force-format': True,
            'push_resources_to_datastore': True
        }

        # Path to the processor we want to test
        processor_dir = \
            os.path.dirname(datapackage_pipelines_ckan.processors.__file__)
        processor_path = os.path.join(processor_dir, 'dump/to_ckan.py')

        # Trigger the processor with our mock `ingest` and capture what it will
        # returned to `spew`.
        json_file = {'first': 'Fred', 'last': 'Smith'}
        json_file = json.dumps(json_file)
        spew_args, _ = mock_dump_test(
            processor_path,
            (params, datapackage,
             iter([ResourceIterator(io.StringIO(json_file),
                                    datapackage['resources'][0],
                                    {'schema': {'fields': []}})
                   ])))

        spew_res_iter = spew_args[1]
        for r in spew_res_iter:
            list(r)  # iterate the row to yield it

        requests = mock_request.request_history
        assert len(requests) == 7
        assert requests[0].url == package_create_url
        assert requests[1].url == resource_create_url
        assert requests[2].url == resource_create_url
        assert requests[3].url == package_show_url
        assert requests[4].url.startswith(datastore_search_url)
        assert requests[5].url == datastore_create_url
        assert requests[6].url == datastore_upsert_url
