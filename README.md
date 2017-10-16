# datapackage-pipelines-ckan

[![Travis](https://img.shields.io/travis/frictionlessdata/datapackage-pipelines-ckan/master.svg)](https://travis-ci.org/frictionlessdata/datapackage-pipelines-ckan)
[![Coveralls](http://img.shields.io/coveralls/frictionlessdata/datapackage-pipelines-ckan/master.svg)](https://coveralls.io/r/frictionlessdata/datapackage-pipelines-ckan?branch=master)
[![PyPi](https://img.shields.io/pypi/v/datapackage-pipelines-ckan.svg)](https://pypi.python.org/pypi/datapackage-pipelines-ckan)
[![SemVer](https://img.shields.io/badge/versions-SemVer-brightgreen.svg)](http://semver.org/)
[![Gitter](https://img.shields.io/gitter/room/frictionlessdata/chat.svg)](https://gitter.im/frictionlessdata/chat)

[Data Package Pipelines](https://github.com/frictionlessdata/datapackage-pipelines) processors for [CKAN](https://ckan.org/).


## Install

```
# clone the repo and install it with pip

git clone https://github.com/frictionlessdata/datapackage-pipelines-ckan.git
pip install -e .
```


## Usage

datapackage-pipelines-ckan contains several pipeline processors for working with CKAN.

### `ckan.add_ckan_resource`

A processor to retrieve metadata about a CKAN resource from a CKAN instance and add it as a datapackage resource.

```yaml
  run: ckan.add_ckan_resource
  parameters:
    ckan-host: http://demo.ckan.org
    resource-id: d51c9bd4-8256-4289-bdd7-962f8572efb0
    ckan-api-key: env:CKAN_API_KEY  # an env var defining a ckan user api key
```

- `ckan-host`: The base url (and scheme) for the CKAN instance (e.g. http://demo.ckan.org).
- `resource-id`: The id of CKAN resource
- `ckan-api-key`: Either a CKAN user api key or, if in the format `env:CKAN_API_KEY_NAME`, an env var that defines an api key. Optional, but necessary for private datasets.

### `ckan.dump.to_ckan`

A processor to save a datapackage and resources to a specified CKAN instance.

```yaml
  run: ckan.dump.to_ckan
  parameters:
    ckan-host: http://demo.ckan.org
    ckan-api-key: env:CKAN_API_KEY
    overwrite_existing: true
    dataset-properties:
      name: test-dataset-010203
      state: draft
      private: true
      owner_org: my-test-org
```

- `ckan-host`: The base url (and scheme) for the CKAN instance (e.g. http://demo.ckan.org).
- `ckan-api-key`: Either a CKAN user api key or, if in the format `env:CKAN_API_KEY_NAME`, an env var that defines an api key.
- `overwrite_existing`: If `true`, if the CKAN dataset already exists, it will be overwritten by the datapackage. Optional, and defaults to `false`.
- `dataset-properties`: An optional object, the properties of which will be used to set properties of the CKAN dataset.
