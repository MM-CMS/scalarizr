import tempfile
import os
import sys
import subprocess
import random
import time
import uuid
import logging
import threading
from ConfigParser import ConfigParser

import mock
from behave import given, when, then

from scalarizr.util import cryptotool
from scalarizr.storage2 import cloudfs
from scalarizr.storage2 import largetransfer
from scalarizr.storage2.cloudfs import LOG
from scalarizr.storage2.largetransfer import TransferError

LOG.setLevel(logging.DEBUG)
LOG.addHandler(logging.FileHandler("transfer_test.log", 'w'))


class FileSource(object):

    def __init__(self, path):
        self.src = path
        self.name = os.path.basename(path)
        self.md5 = cryptotool.calculate_md5_sum(path)


class StreamSource(object):

    def __init__(self, path):
        self.src = open(path, 'rb')
        self.md5 = cryptotool.calculate_md5_sum(path)


def convert_manifest(json_manifest):
    assert len(json_manifest["files"]) == 1, json_manifest.data
    assert json_manifest["files"][0]["compressor"] == "gzip"

    parser = ConfigParser()
    parser.add_section("snapshot")
    parser.add_section("chunks")

    parser.set("snapshot", "description", json_manifest["description"])
    parser.set("snapshot", "created_at", json_manifest["created_at"])
    parser.set("snapshot", "pack_method", json_manifest["files"][0]["compressor"])

    for chunk, md5sum, size in reversed(json_manifest["files"][0]["chunks"]):
        parser.set("chunks", chunk, md5sum)

    LOG.debug("CONVERT: %s", parser.items("chunks"))
    return parser


def make_file(base_dir, name=None, size=None):
    name = name or uuid.uuid4().hex
    file_path = os.path.join(base_dir, name)
    if size is None:
        size = random.randint(2, 20)
    if not os.path.exists(base_dir):
        os.mkdir(base_dir)
    subprocess.call([
            "dd",
            "if=/dev/urandom",
            "of=%s" % file_path,
            "bs=1M",
            "count=%s" % size
    ], stdout=open('/dev/null', 'w'), stderr=subprocess.STDOUT, close_fds=True)
    LOG.debug('Make file %s, %s' % (file_path, size))
    assert os.path.exists(file_path), file_path
    return file_path, size


def make_file_source(base_dir, size=None):
    file_path, size = make_file(base_dir, size=size)
    return FileSource(file_path)


def make_stream_source(base_dir):
    file_path, size = make_file(base_dir)
    return StreamSource(file_path)


@given(u"I have {number:d} file(s)")
def i_have_file(context, number):
    for i in range(number):
        context.sources.append(make_file_source(context.tmp_dir))


@given(u"I have {size:d}MB file")
def i_have_file_with_size(context, size):
    context.sources.append(make_file_source(context.tmp_dir, size=size))


@given(u"I have {number:d} remote file(s)")
def i_have_remote_file(context, number):
    for i in range(number):
        context.sources.append(make_file_source(context.remote_dir))


@given(u"I have {number:d} stream(s)")
def i_have_stream(context, number):
    for i in range(number):
        context.sources.append(make_stream_source(context.tmp_dir))


@when(u"I upload file(s) to Storage with simple {simple}")
def i_upload_file(context, simple):
    simple == 'True'
    sources = [source.src for source in context.sources]
    context.tr = largetransfer.Upload(sources, context.remote_dir, simple=simple)
    context.tr.apply_async()
    try:
        context.tr.join()
    except:
        context.error = sys.exc_info()


@when(u"I upload stream(s) to Storage with gzipping {gzip}")
def i_upload_stream_with_gzipping(context, gzip):
    gzip == 'True'
    src = [cloudfs.NamedStream(stream.src, os.path.basename(stream.src.name)) for stream in context.sources]
    for s in src:
        LOG.debug(s.name)
    time.sleep(1)
    if len(src) == 1:
        src = src[0]
    context.tr = largetransfer.Upload(src, context.remote_dir, gzip=gzip, chunk_size=5)
    context.tr.apply_async()
    try:
        context.tr.join()
    except:
        context.error = sys.exc_info()
    context.manifest = context.tr.manifest
    context.gzip = True


@then(u"I expect manifest as a result")
def i_expect_manifest_as_a_result(context):
    assert context.manifest
    assert context.manifest['files']
    for f in context.manifest['files']:
        if context.gzip:
            assert f['compressor'] == 'gzip'
        else:
            assert f['compressor'] == ''
    assert context.driver.exists(context.manifest.cloudfs_path), context.manifest.cloudfs_path


@then(u"All data are uploaded")
def all_data_are_uploaded(context):
    if context.manifest:
        check_for_duplicate = []
        for f in context.manifest['files']:
            for chunk in f['chunks']:
                assert chunk[0] not in check_for_duplicate, 'Duplicate chunk: %s' % chunk[0]
                chunk_url = os.path.join(os.path.dirname(context.manifest.cloudfs_path), chunk[0])
                check_for_duplicate.append(chunk[0])
                assert context.driver.exists(chunk_url), 'Chunk not exists: %s' % chunk_url
    else:
        for source in context.sources:
            remote_path = os.path.join(context.remote_dir, source.name)
            assert context.driver.exists(remote_path), remote_path


@then(u"I expect there is no error")
def i_expect_there_is_no_error(context):
    assert context.error is None, context.error


@given(u"I have uploaded stream with gzipping {gzip}")
def i_have_uploaded_stream(context, gzip):
    context.execute_steps(u'''
            Given I have 1 stream(s)
            When I upload stream(s) to Storage with gzipping {gzip}
            Then I expect there is no error
            Then I expect manifest as a result
            And all data are uploaded
    '''.format(gzip=gzip))


@given(u"I have uploaded stream with old manifest")
def i_have_uploaded_stream_with_old_manifest(context):
    context.execute_steps(u'''
            Given I have 1 stream(s)
            When I upload stream(s) to Storage with gzipping True
            Then I expect there is no error
            Then I expect manifest as a result
            And all data are uploaded
    ''')
    manifest_ini_path = os.path.join(context.tmp_dir, "manifest.ini")
    with open(manifest_ini_path, 'w') as fd:
        convert_manifest(context.manifest).write(fd)
    context.driver.delete(context.manifest.cloudfs_path)
    destination = os.path.join(os.path.dirname(context.manifest.cloudfs_path), 'manifest.ini')
    context.manifest.cloudfs_path = context.driver.put(manifest_ini_path, destination)


@given(u"I have error {error} in driver")
def i_have_error_in_driver(context, error):
    scheme = context.remote_dir.split('://')[0]
    context.mock_driver = mock.MagicMock()
    cloudfs.cloudfs_types[scheme] = context.mock_driver
    context.mock_driver.side_effect = eval(error)


@when(u"I download with the manifest")
def i_download_with_manifest(context):
    manifest_url = context.manifest.cloudfs_path
    context.tr = largetransfer.Download(manifest_url)
    context.tr.apply_async()
    tmp_dir = tempfile.mkdtemp()
    with open(os.path.join(tmp_dir, 'output'), 'wb') as f:
        while True:
            data = context.tr.output.read(4096)
            if not data:
                break
            f.write(data)
    try:
        context.tr.join()
    except:
        context.error = sys.exc_info()
    context.downloaded_files = [os.path.join(tmp_dir, 'output')]


@then(u"I expect original items are downloaded")
def i_expect_original_items_downloaded(context):
    sources_md5_sum = [source.md5 for source in context.sources]
    for f in context.downloaded_files:
        assert_msg = 'md5(%s) not in %s' % (f, sources_md5_sum)
        assert cryptotool.calculate_md5_sum(f) in sources_md5_sum, assert_msg


@then(u"I get TransferError {error}")
def check_error(context, error):
    assert context.error
    assert context.error[0] == type(context.tr.error) == TransferError
    assert context.tr.error.args[0] == error, context.tr.error.args[0]


@when(u"I remove one chunk")
def i_remove_one_chunk(context):
    remote_dir = os.path.dirname(context.manifest.cloudfs_path)
    for f in context.manifest['files']:
        path = os.path.join(remote_dir, random.choice(f['chunks'])[0])
        context.driver.delete(path)
        assert not context.driver.exists(path)


@when(u"I start upload file(s) to Storage with simple {simple}")
def i_start_upload(context, simple):
    simple == 'True'
    sources = [source.src for source in context.sources]
    context.tr = largetransfer.Upload(sources, context.remote_dir, simple=simple)
    context.tr.apply_async()


@then(u"I stop upload")
def i_stop_upload(context):
    t = threading.Thread(target=context.tr.stop)
    t.start()
    t.join(5)
    if t.is_alive():
        context.tr.terminate()
        assert False
    else:
        assert True


@then(u"I expect uploading is stopped")
def i_expect_uploading_stopped(context):
    assert not context.tr.running


@then(u"I wait while upload starts")
def wait_start(context):
    while not context.tr.running:
        time.sleep(0.1)


@then(u"I wait {second:d} second(s)")
def wait(context, second):
    time.sleep(second)


@when(u"I replace one chunk")
def replace_chunk(context):
    remote_dir = os.path.dirname(context.manifest.cloudfs_path)
    random_file = random.randint(0, len(context.manifest['files']) - 1)
    random_chunk_for_replace = context.manifest['files'][random_file]['chunks'][-1][0]
    context.driver.delete(os.path.join(remote_dir, random_chunk_for_replace))
    new_file, size = make_file(context.tmp_dir, name=random_chunk_for_replace, size=1)
    context.driver.put(new_file, remote_dir)


@then(u"Temporary files are deleted")
def temorary_files_are_deleted(context):
    assert context.tr._tmp_dir
    assert not os.path.exists(context.tr._tmp_dir)


@when(u"I download with old LargeTransfer")
def download_with_old_transfer(context):
    src = context.manifest.cloudfs_path
    dst = os.path.join(context.tmp_dir, 'output')
    os.mkdir(dst)
    tr = cloudfs.LargeTransfer(src, dst)
    tr.run()
    context.downloaded_files = [os.path.join(dst, os.listdir(dst)[0])]


@given(u"I have uploaded file with simple {simple}")
def i_have_uploaded_file(context, simple):
    context.execute_steps(u'''
            Given I have 1 file(s)
            When I upload file(s) to Storage with simple {simple}
            Then I expect there is no error
            Then All data are uploaded
            And Temporary files are deleted
    '''.format(simple=simple))


@when(u"I download file")
def download_file(context):
    sources = [os.path.join(context.remote_dir, source.name) for source in context.sources]
    context.tr = largetransfer.Download(sources, context.tmp_dir, simple=True)
    context.tr.apply_async()
    try:
        context.tr.join()
    except:
        context.error = sys.exc_info()
    context.downloaded_files = [os.path.join(context.tmp_dir, source.src)
                                for source in context.sources]
