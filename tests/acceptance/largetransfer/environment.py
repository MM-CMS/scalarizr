"""
Essential environment variables:
$AWS_ACCESS_KEY_ID, $AWS_SECRET_ACCESS_KEY for s3
$OS_USERNAME, $OS_PASSWORD for swift;
$ENTER_IT_USERNAME, $ENTER_IT_API_KEY for swift enter.it

default storage to test is local; override with $LT_TEST_STORAGE
"""


import os
import shutil
import tempfile
import mock
import logging

import boto
import swiftclient

from scalarizr.storage2 import cloudfs
from scalarizr.storage2.cloudfs import s3, gcs, swift, local
from scalarizr.storage2.cloudfs import LOG

LOG.setLevel(logging.DEBUG)
LOG.addHandler(logging.FileHandler("transfer_test.log", 'w'))


import httplib2
from apiclient.discovery import build
from oauth2client.client import SignedJwtAssertionCredentials


def get_s3_conn():
    conn = boto.connect_s3(
                          host='s3.amazonaws.com',
                          aws_access_key_id=os.environ['EC2_ACCESS_KEY'],
                          aws_secret_access_key=os.environ['EC2_SECRET_KEY'])
    return conn


def get_gcs_conn():
    gce_storage_full_scope = (
                             "https://www.googleapis.com/auth/devstorage.full_control",
                             "https://www.googleapis.com/auth/devstorage.read_only",
                             "https://www.googleapis.com/auth/devstorage.read_write",
                             "https://www.googleapis.com/auth/devstorage.write_only")
    email = '876103924605@developer.gserviceaccount.com'
    with open('3bbdf754707f053f12fd17c78a8cfb1cee34736e-privatekey.p12', 'rb') as f:
        pk = f.read()
    cred = SignedJwtAssertionCredentials(email, pk, gce_storage_full_scope)
    http = httplib2.Http()
    http = cred.authorize(http)
    conn = build('storage', 'v1', http=http)
    return conn


def get_swift_conn():
    user = os.environ['OS_USERNAME']
    passwd = os.environ['OS_PASSWORD']
    auth_url = 'https://identity.api.rackspacecloud.com/v1.0'
    kwds = {
            'auth_version': '1',
            'tenant_name': 'sebastianstadil'
    }
    conn = swiftclient.Connection(
        authurl=auth_url,
        user=user,
        key=passwd,
        **kwds
    )
    return conn


def before_feature(context, feature):
    context.storages = {
            "s3": {
                    "url": "s3://scalr-lt-test",
                    "driver": s3.S3FileSystem,
            },
            "gcs": {
                    "url": "gcs://scalr-lt-test",
                    "driver": gcs.GCSFileSystem,
            },
            "swift": {
                    "url": "swift://scalr-lt-test",
                    "driver": swift.SwiftFileSystem,
            },
            "swift-enter-it": {
                    "url": "swift://scalr-lt-test",
                    "driver": swift.SwiftFileSystem,
            },
            "local": {
                    "url": "file:///tmp/cloudfs",
                    "driver": local.LocalFileSystem,
            }
    }
    if "LT_TEST_STORAGE" in os.environ:
        context.storage = os.environ["LT_TEST_STORAGE"]
    else:
        context.storage = "local"
    context.driver = context.storages[context.storage]["driver"]()
    context.manifest = None

    # mock bus and driver
    gcs.bus = mock.MagicMock()
    if context.storage == 's3':
        context.driver.get_s3_conn = get_s3_conn
        s3.__node__['ec2'].connect_s3 = get_s3_conn
        cloudfs.cloudfs_types['s3'].get_s3_conn = get_s3_conn
        s3.S3FileSystem._get_connection = lambda self: get_s3_conn()
        s3.S3FileSystem._bucket_location = lambda self: ''
    elif context.storage == "gcs":
        context.driver.get_storage_conn = get_gcs_conn
        gcs.bus.platform.get_numeric_project_id.return_value = '876103924605'
        gcs.bus.platform.get_storage_conn = get_gcs_conn
        cloudfs.cloudfs_types['gcs'].get_storage_conn = get_gcs_conn
    elif context.storage == 'swift':
        context.driver.get_swift_conn = get_swift_conn
        swift.__node__['openstack'].connect_swift = get_swift_conn
        cloudfs.cloudfs_types['swift'].get_swift_conn = get_swift_conn


def after_feature(context, feature):
    pass


def before_scenario(context, scenario):
    context.mock_driver = None
    context.sources = []
    context.remote_dir = context.storages[context.storage]["url"]
    context.tmp_dir = tempfile.mkdtemp()
    context.error = None
    if context.storage == 'local':
        shutil.rmtree(context.remote_dir.split('://')[-1], ignore_errors=True)


def after_scenario(context, scenario):
    if context.manifest:
        context.manifest.delete()
        context.driver.delete(context.manifest.cloudfs_path)
    for f in context.driver.ls(context.remote_dir):
        context.driver.delete(f)
    shutil.rmtree(context.tmp_dir)
    if context.mock_driver:
        scheme = context.remote_dir.split('://')[0]
        cloudfs.cloudfs_types[scheme] = context.storages[context.storage]["driver"]
