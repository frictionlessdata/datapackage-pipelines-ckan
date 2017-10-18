import os
import json
import unittest

import requests_mock

from datapackage_pipelines.utilities.lib_test_helpers import (
    mock_processor_test
)

import datapackage_pipelines_ckan.processors

# mock the ckan resource response
MOCK_CKAN_RESPONSE = {
    'help': 'https://demo.ckan.org/api/3/action/help_show?name=resource_show',
    'success': True,
    'result': {
        'mimetype': 'text/csv',
        'cache_url': None,
        'hash': '224ba4b7482d3cdd7e6b2373679a9cfaf8eb8dac',
        'description': '',
        'name': 'January 2012',
        'format': 'CSV',
        'url': 'http://www.newcastle.gov.uk/sites/drupalncc.newcastle.gov.uk/files/wwwfileroot/your-council/local_transparency/january_2012.csv',  # noqa
        'datastore_active': True,
        'cache_last_updated': None,
        'package_id': 'b9076e40-80ea-480b-b330-e399d7a8c09b',
        'created': '2012-08-14T12:39:24.519707',
        'state': 'active',
        'mimetype_inner': '',
        'last_modified': '2012-08-14T12:39:35.638235',
        'position': 4,
        'revision_id': '746c4bc2-7666-4519-a008-13ed98161b03',
        'url_type': None,
        'id': 'd51c9bd4-8256-4289-bdd7-962f8572efb0',
        'resource_type': 'file',
        'size': '1080880'
    }
}

MOCK_CKAN_NOT_FOUND = {
    'success': False,
    'error':  {
        'message': 'Not found: Resource was not found.',
        '__type': 'Not Found Error'
    }
}

MOCK_CKAN_ERROR = {
    'success': False,
    'error': {
        'message': 'Access denied: User  not authorized to read resource 0d8b3df4-4771-44bf-8c56-db9a12f1a64a',  # noqa
        '__type': 'Authorization Error'
    }
}


class TestAddCkanResourceProcessor(unittest.TestCase):

    @requests_mock.mock()
    def test_add_ckan_resource_processor(self, mock_request):

        mock_request.get('https://demo.ckan.org/api/3/action/resource_show',
                         json=MOCK_CKAN_RESPONSE)

        # input arguments used by our mock `ingest`
        datapackage = {
            'name': 'my-datapackage',
            'project': 'my-project',
            'resources': []
        }
        params = {
            'ckan-host': 'https://demo.ckan.org',
            'resource-id': 'd51c9bd4-8256-4289-bdd7-962f8572efb0'
        }

        # Path to the processor we want to test
        processor_dir = \
            os.path.dirname(datapackage_pipelines_ckan.processors.__file__)
        processor_path = os.path.join(processor_dir, 'add_ckan_resource.py')

        # Trigger the processor with our mock `ingest` and capture what it will
        # returned to `spew`.
        spew_args, _ = mock_processor_test(processor_path,
                                           (params, datapackage, []))

        spew_dp = spew_args[0]
        spew_res_iter = spew_args[1]

        # Asserts for the datapackage
        dp_resources = spew_dp['resources']
        assert len(dp_resources) == 1
        assert dp_resources[0]['name'] == 'january-2012'
        assert dp_resources[0]['format'] == 'csv'
        assert dp_resources[0]['dpp:streamedFrom'] == \
            MOCK_CKAN_RESPONSE['result']['url']
        assert 'schema' not in dp_resources[0]

        # Asserts for the res_iter
        spew_res_iter_contents = list(spew_res_iter)
        assert len(spew_res_iter_contents) == 0

    @requests_mock.mock()
    def test_add_ckan_resource_processor_api_key(self, mock_request):

        mock_request.get('https://demo.ckan.org/api/3/action/resource_show',
                         json=MOCK_CKAN_RESPONSE)

        # input arguments used by our mock `ingest`
        datapackage = {
            'name': 'my-datapackage',
            'project': 'my-project',
            'resources': []
        }
        params = {
            'ckan-host': 'https://demo.ckan.org',
            'resource-id': 'd51c9bd4-8256-4289-bdd7-962f8572efb0',
            'ckan-api-key': 'my-api-key'
        }

        # Path to the processor we want to test
        processor_dir = \
            os.path.dirname(datapackage_pipelines_ckan.processors.__file__)
        processor_path = os.path.join(processor_dir, 'add_ckan_resource.py')

        # Trigger the processor with our mock `ingest` and capture what it will
        # returned to `spew`.
        spew_args, _ = mock_processor_test(processor_path,
                                           (params, datapackage, []))

        # test request contained a Authorization header with our api key
        request_history = mock_request.request_history
        assert request_history[0].headers['Authorization'] == 'my-api-key'

    @requests_mock.mock()
    def test_add_ckan_resource_processor_invalid_json(self, mock_request):

        mock_request.get('https://demo.ckan.org/api/3/action/resource_show',
                         text='nope')

        # input arguments used by our mock `ingest`
        datapackage = {
            'name': 'my-datapackage',
            'project': 'my-project',
            'resources': []
        }
        params = {
            'ckan-host': 'https://demo.ckan.org',
            'resource-id': 'd51c9bd4-8256-4289-bdd7-962f8572efb0'
        }

        # Path to the processor we want to test
        processor_dir = \
            os.path.dirname(datapackage_pipelines_ckan.processors.__file__)
        processor_path = os.path.join(processor_dir, 'add_ckan_resource.py')

        # Trigger the processor with our mock `ingest` will return an exception
        with self.assertRaises(json.decoder.JSONDecodeError):
            spew_args, _ = mock_processor_test(processor_path,
                                               (params, datapackage, []))

    @requests_mock.mock()
    def test_add_ckan_resource_processor_not_found(self, mock_request):

        mock_request.get('https://demo.ckan.org/api/3/action/resource_show',
                         json=MOCK_CKAN_NOT_FOUND)

        # input arguments used by our mock `ingest`
        datapackage = {
            'name': 'my-datapackage',
            'project': 'my-project',
            'resources': []
        }
        params = {
            'ckan-host': 'https://demo.ckan.org',
            'resource-id': 'd51c9bd4-8256-4289-bdd7-962f8572efb0'
        }

        # Path to the processor we want to test
        processor_dir = \
            os.path.dirname(datapackage_pipelines_ckan.processors.__file__)
        processor_path = os.path.join(processor_dir, 'add_ckan_resource.py')

        # Trigger the processor with our mock `ingest` will return an exception
        with self.assertRaises(Exception):
            spew_args, _ = mock_processor_test(processor_path,
                                               (params, datapackage, []))

    @requests_mock.mock()
    def test_add_ckan_resource_processor_misc_error(self, mock_request):

        mock_request.get('https://demo.ckan.org/api/3/action/resource_show',
                         json=MOCK_CKAN_ERROR)

        # input arguments used by our mock `ingest`
        datapackage = {
            'name': 'my-datapackage',
            'project': 'my-project',
            'resources': []
        }
        params = {
            'ckan-host': 'https://demo.ckan.org',
            'resource-id': 'd51c9bd4-8256-4289-bdd7-962f8572efb0'
        }

        # Path to the processor we want to test
        processor_dir = \
            os.path.dirname(datapackage_pipelines_ckan.processors.__file__)
        processor_path = os.path.join(processor_dir, 'add_ckan_resource.py')

        # Trigger the processor with our mock `ingest` will return an exception
        with self.assertRaises(Exception):
            spew_args, _ = mock_processor_test(processor_path,
                                               (params, datapackage, []))
