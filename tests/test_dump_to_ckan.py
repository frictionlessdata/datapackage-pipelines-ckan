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
                "path": "."
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
