import os
import mock
import time
import nose
import uuid
import Queue
import shutil
import random
import thread
import logging
import tempfile
import threading
import subprocess
import multiprocessing

from scalarizr.storage2 import largetransfer
from scalarizr.storage2.cloudfs import cloudfs_types, local
from scalarizr.util import cryptotool

from scalarizr.storage2.largetransfer import LOG


prompt = "[%(asctime)15s][%(module)20s][%(process)d] %(levelname)10s %(message)s"
frmtr = logging.Formatter(prompt, datefmt='%d/%b/%Y %H:%M:%S')
hndlr = logging.FileHandler("transfer_test.log", 'w')
hndlr.setFormatter(frmtr)
LOG.addHandler(hndlr)


tmp_dir = None


def create_tmp_dir():
    global tmp_dir
    tmp_dir = tempfile.mkdtemp()


def remove_tmp_dir():
    if tmp_dir and os.path.exists(tmp_dir):
        shutil.rmtree(tmp_dir)


def make_file(name=None, size=None):
    name = name or uuid.uuid4().hex
    file_path = os.path.join(tmp_dir, name)
    if size is None:
        size = random.randint(2, 20)
    size_bytes = size * 1024 * 1024
    subprocess.call([
        "dd",
        "if=/dev/urandom",
        "of=%s" % file_path,
        "bs=1M",
        "count=%s" % size
    ], stdout=open('/dev/null', 'w'), stderr=subprocess.STDOUT, close_fds=True)
    md5_sum = cryptotool.calculate_md5_sum(file_path)
    gotten_size = os.path.getsize(file_path)
    assert gotten_size == size_bytes, gotten_size
    LOG.debug('create file %s with size %sMB, md5sum %s' % (file_path, size, md5_sum))
    return file_path, size_bytes, md5_sum


@nose.with_setup(create_tmp_dir, remove_tmp_dir)
def test_split():
    data_to_test = [
        {'size': 21, 'chunk_size': None, 'extension': None},
        {'size': 21, 'chunk_size': 2, 'extension': None},
        {'size': 21, 'chunk_size': None, 'extension': 'gz'},
        {'size': 21, 'chunk_size': None, 'extension': None},
    ]

    for data in data_to_test:
        size = data['size']
        chunk_size = data['chunk_size']
        extension = data['extension']

        file_path, size, md5_sum = make_file(size=size)
        with open(file_path, 'rb') as stream:
            splitter = largetransfer.split(
                stream, tmp_dir, chunk_size=chunk_size, extension=extension)
            chunks_info = [chunk_info for chunk_info in splitter]

            # check size
            for chunk_info in chunks_info[0:-1]:
                expected = (chunk_size or largetransfer.DEFAULT_CHUNK_SIZE) * 1024 * 1024
                gotten = chunk_info.size
                msg = 'wrong %s size, expected %s, gotten %s' % (chunk_info.name, expected, gotten)
                assert expected == gotten, msg

            # check name
            name_template = os.path.basename(file_path)
            if extension:
                name_template = name_template + '.' + extension
            name_template += '.%03d'
            for chunk_idx, chunk_info in enumerate(chunks_info):
                msg = 'wrong chunk name, expected %s, gotten %s' %\
                    (name_template % chunk_idx, chunk_info.name)
                assert chunk_info.name == name_template % chunk_idx, msg


class TestFileInfo(object):

    def setup(self):
        create_tmp_dir()

    def teardown(self):
        remove_tmp_dir()

    def test__init__default(self):
        file_path, size, md5_sum = make_file(name='test')
        file_info = largetransfer.FileInfo(file_path)
        msg = 'wrong name, expected %s, gotten %s' % ('test', file_info.name)
        assert file_info.name == 'test', msg
        msg = 'wrong size, expected %s, gotten %s' % (size, file_info.size)
        assert file_info.size == size, msg
        msg = 'wrong md5sum, expected %s, gotten %s' % (md5_sum, file_info.md5_sum)
        assert file_info.md5_sum == md5_sum, msg

    def test__init__custom(self):
        file_path, size, md5_sum = make_file(name='test')
        file_info = largetransfer.FileInfo(file_path, size=size, md5_sum=md5_sum)
        assert file_info.name == 'test', file_info.name
        msg = 'wrong size, expected %s, gotten %s' % (size, file_info.size)
        assert file_info.size == size, msg
        msg = 'wrong md5sum, expected %s, gotten %s' % (md5_sum, file_info.md5_sum)
        assert file_info.md5_sum == md5_sum, msg


class TestNonBlockingLifoQueue(object):

    def test_join(self):
        """Test NonBlockignLifoQueue.join() method is interrupted"""

        def foo():
            time.sleep(2)
            thread.interrupt_main()

        queue = largetransfer.NonBlockingLifoQueue()
        queue.put('Something')
        interrupted = False
        t = threading.Thread(target=foo)
        t.start()
        try:
            t.join(5)
        except KeyboardInterrupt:
            interrupted = True
        except:
            pass
        assert interrupted


class Test_Worker(object):

    def test_start(self):

        def foo():
            raise Exception()

        queue = largetransfer.NonBlockingLifoQueue()

        task_for_interrupt = {
            'fn': foo,
            'args': (),
            'kwds': {},
            'retry': 0,
            'status': 'submitted',
            'error': None,
            'result': None,
            'complete_cb': None,
        }

        worker = largetransfer._Worker(queue)
        assert worker.status == 'stopped'
        assert not worker._start_ev.is_set()
        t = threading.Thread(target=worker.start)
        t.start()
        try:
            time.sleep(1)
            assert worker.status == 'running'
            assert worker._start_ev.is_set()
        finally:
            queue.put(task_for_interrupt)
            try:
                t.join(1)
            except KeyboardInterrupt:
                assert True
            except:
                assert False
            finally:
                t.join(1)
            assert not t.is_alive()

    def test_stop(self):
        queue = largetransfer.NonBlockingLifoQueue()
        worker = largetransfer._Worker(queue)
        t = threading.Thread(target=worker.start)
        t.start()
        worker.wait_start()
        worker.stop()
        assert worker.status == 'stopped'
        t.join(1)
        assert not worker._start_ev.is_set()
        assert not t.is_alive()


class Test_Transfer(object):

    def test__init__default(self):
        _tr = largetransfer._Transfer('put')
        assert _tr._pool_size == largetransfer.DEFAULT_POOL_SIZE
        assert len(_tr._workers) == largetransfer.DEFAULT_POOL_SIZE
        for w in _tr._workers:
            assert w.status == 'running'

        #  stop workers after test
        for w in _tr._workers:
            w.stop()

    def test__init__custom(self):
        _tr = largetransfer._Transfer('put', pool_size=3)
        assert _tr._pool_size == 3
        assert len(_tr._workers) == 3
        for w in _tr._workers:
            assert w.status == 'running'

        #  stop workers after test
        for w in _tr._workers:
            w.stop()

    def test_stop(self):
        _tr = largetransfer._Transfer('put')
        _tr.stop()
        for w in _tr._workers:
            assert w.status == 'stopped'

    def test_stop_with_wait(self):
        _tr = largetransfer._Transfer('put')
        _tr.stop(wait=True)
        for w in _tr._workers:
            assert w.status == 'stopped'


class TestUpload(object):

    _origin_put = cloudfs_types['file'].put

    def setup(self):
        create_tmp_dir()

    def teardown(self):
        remove_tmp_dir()
        cloudfs_types['file'].put = self._origin_put

    def test__init__default(self):
        upload = largetransfer.Upload(['src'], 'dst')
        assert upload.gzip is True
        assert upload.use_pigz is True
        assert upload._pool_size == largetransfer.DEFAULT_POOL_SIZE
        assert upload._chunk_size == largetransfer.DEFAULT_CHUNK_SIZE
        assert upload._simple is False
        assert upload._cb_interval == largetransfer.DEFAULT_CALLBACK_INTERVAL
        assert hasattr(upload.src, '__iter__')
        assert upload._manifest is not None
        assert upload._callback_thread is None

    def test__init__single_src(self):
        upload = largetransfer.Upload('src', 'dst')
        assert hasattr(upload.src, '__iter__')
        assert upload._manifest is not None
        assert upload._simple is False
        assert upload._callback_thread is None

    def test__init__list_src(self):
        upload = largetransfer.Upload(['src'], 'dst')
        assert hasattr(upload.src, '__iter__')
        assert upload._manifest is not None
        assert upload._simple is False
        assert upload._callback_thread is None

    def test__init__simple(self):
        upload = largetransfer.Upload(['src'], 'dst', simple=True)
        assert hasattr(upload.src, '__iter__')
        assert upload._manifest is None
        assert upload._simple is True
        assert upload._callback_thread is None

    def test__init__custom(self):

        def progress_cb(progres):
            return

        upload = largetransfer.Upload(['src'], 'dst', transfer_id='xxx', description='desc',
                                      tags='tags', gzip=False, use_pigz=False, pool_size=1,
                                      chunk_size=10, progress_cb=progress_cb, cb_interval=10)
        assert upload.gzip is False
        assert upload.use_pigz is False
        assert upload._pool_size == 1
        assert upload._chunk_size == 10
        assert upload._simple is False
        assert upload._cb_interval == 10
        assert upload._progress_cb == progress_cb
        assert hasattr(upload.src, '__iter__')
        assert upload._manifest is not None
        assert upload._manifest['description'] == 'desc'
        assert upload._manifest['tags'] == 'tags'
        assert upload.dst == os.path.join('dst', 'xxx')
        assert upload._callback_thread is None

    def test_not_running(self):
        upload = largetransfer.Upload(['src'], 'dst')
        assert not upload.running

    @mock.patch('scalarizr.storage2.cloudfs.cloudfs')
    def test_apply_async1(self, cloudfs_mock):
        file_path, size, md5_sum = make_file()
        stream = open(file_path, 'rb')
        upload = largetransfer.Upload([stream], tmp_dir, chunk_size=2)
        upload.apply_async()
        time.sleep(1)
        assert not upload.running

    def test_apply_async2(self):
        """
        Test apply_async method without progress callback function
        """
        def put(*args, **kwds):
            time.sleep(2)

        cloudfs_types['file'].put = mock.MagicMock(side_effect=put)
        file_path, size, md5_sum = make_file()
        stream = open(file_path, 'rb')
        upload = largetransfer.Upload([stream], tmp_dir, chunk_size=2)
        upload.apply_async()

        assert upload.running
        assert upload._callback_thread is None

    def test_apply_async3(self):
        """
        Test apply_async method with progress callback function
        """
        def put(*args, **kwds):
            time.sleep(2)

        cloudfs_types['file'].put = mock.MagicMock(side_effect=put)
        file_path, size, md5_sum = make_file()
        stream = open(file_path, 'rb')

        def progress_cb(progress):
            return

        upload = largetransfer.Upload([stream], tmp_dir, chunk_size=2, progress_cb=progress_cb)
        upload.apply_async()

        assert upload.running
        assert upload._callback_thread
        assert upload._callback_thread.is_alive()

        upload.join()

    def test_stop1(self):
        """
        Test stop method when upload is not running
        """
        upload = largetransfer.Upload('src', 'dst')
        upload._kill = mock.MagicMock()
        upload.stop()
        assert upload._kill.call_count == 0

    def test_stop2(self):
        """
        Test stop method when upload is blocked
        """
        def put(*args, **kwds):
            time.sleep(30)

        cloudfs_types['file'].put = mock.MagicMock(side_effect=put)
        file_path, size, md5_sum = make_file()
        stream = open(file_path, 'rb')

        def progress_cb(progress):
            return

        upload = largetransfer.Upload([stream], tmp_dir, chunk_size=2, progress_cb=progress_cb)
        upload.apply_async()

        # wait start
        time.sleep(5)

        assert upload.error is None, upload.error

        assert upload.running
        assert upload._callback_thread
        assert upload._callback_thread.is_alive()

        upload.stop()

        assert upload.process is None
        assert not upload.running
        assert upload._callback_thread is None

    def test_stop3(self):
        """
        Test stop method when upload is blocked
        """
        def put(*args, **kwds):
            # call blocking method wait()
            threading.Event().wait()

        cloudfs_types['file'].put = mock.MagicMock(side_effect=put)
        file_path, size, md5_sum = make_file()
        stream = open(file_path, 'rb')

        def progress_cb(progress):
            return

        upload = largetransfer.Upload([stream], tmp_dir, chunk_size=2, progress_cb=progress_cb)
        upload.apply_async()

        # wait start
        time.sleep(5)

        assert upload.error is None, upload.error

        assert upload.running
        assert upload._callback_thread
        assert upload._callback_thread.is_alive()

        upload.stop()

        assert upload.process is None
        assert not upload.running
        assert upload._callback_thread is None

    def test_terminate1(self):
        """
        Test terminate method then upload is not running
        """
        upload = largetransfer.Upload('src', 'dst')
        upload._kill = mock.MagicMock()
        upload.terminate()
        msg = 'Wrong call count for upload._kill method, expected %s, gotten %s' %\
            (0, upload._kill.call_count)
        assert upload._kill.call_count == 0, msg

    def test_terminate2(self):
        """
        Test terminate method then upload is blocked
        """
        def put(*args, **kwds):
            # call blocking method wait()
            threading.Event().wait()

        cloudfs_types['file'].put = mock.MagicMock(side_effect=put)
        file_path, size, md5_sum = make_file()
        stream = open(file_path, 'rb')

        def progress_cb(progress):
            return

        upload = largetransfer.Upload([stream], tmp_dir, chunk_size=2, progress_cb=progress_cb)
        upload.apply_async()

        # wait start
        time.sleep(5)

        assert upload.error is None, upload.error

        assert upload.running
        assert upload._callback_thread
        assert upload._callback_thread.is_alive()

        upload.terminate()

        assert upload.process is None
        assert not upload.running
        assert upload._callback_thread is None

    @mock.patch('scalarizr.storage2.cloudfs.cloudfs')
    @mock.patch('scalarizr.storage2.largetransfer.multiprocessing.Queue')
    def test_error1(self, cloudfs_mock, queue_mock):
        upload = largetransfer.Upload(['src'], tmp_dir)
        upload._error_queue = mock.MagicMock()
        upload.apply_async()
        assert upload._error is None
        upload.error
        upload._error_queue.get.assert_called_once()
        upload.error
        upload.error
        msg = 'Wrong call count for upload._error_queue.get method, expected %s, gotten %s' %\
            (3, upload._error_queue.get.call_count)
        assert upload._error_queue.get.call_count == 3, msg

    @mock.patch('scalarizr.storage2.cloudfs.cloudfs')
    @mock.patch('scalarizr.storage2.largetransfer.multiprocessing.Queue')
    def test_error2(self, cloudfs_mock, queue_mock):

        def side_effect(*args, **kwds):
            raise Queue.Empty()

        upload = largetransfer.Upload(['src'], tmp_dir)
        upload._error_queue = mock.MagicMock()
        upload._error_queue.get.side_effect = side_effect
        upload.apply_async()
        assert upload._error is None

        upload.error
        upload._error_queue.get.assert_called_once()
        upload.error
        upload.error
        msg = 'Wrong call count for upload._error_queue.get method, expected %s, gotten %s' %\
            (3, upload._error_queue.get.call_count)
        assert upload._error_queue.get.call_count == 3, msg

    def test_join1(self):
        """
        Test join method then upload is not started
        """
        upload = largetransfer.Upload('src', 'dst')
        upload.join()

    def test_join2(self):
        """
        Test join method then upload is started
        """
        def put(*args, **kwds):
            time.sleep(2)

        cloudfs_types['file'].put = mock.MagicMock(side_effect=put)
        file_path, size, md5_sum = make_file()
        stream = open(file_path, 'rb')

        upload = largetransfer.Upload([stream], tmp_dir, chunk_size=2)
        upload.apply_async()
        upload.join()

        assert upload.error is None
        assert upload.process is None

    def test_join3(self):
        """
        Test join method raises exception
        """
        def put(*args, **kwds):
            raise Exception('Error message')

        cloudfs_types['file'].put = mock.MagicMock(side_effect=put)
        file_path, size, md5_sum = make_file()
        stream = open(file_path, 'rb')

        upload = largetransfer.Upload([stream], tmp_dir, chunk_size=2)
        upload.apply_async()
        try:
            upload.join()
        except largetransfer.TransferError as e:
            assert len(e.args) == 3
            assert e.args[0] == 'Exception'
            assert e.args[1] == 'Error message'
        else:
            assert False

    def test_initial_progress(self):
        upload = largetransfer.Upload('src', 'dst')
        assert upload.progress == 0

    def test_progress(self):
        assert_flag = multiprocessing.Value('i', 0)
        progress_list = []

        def put(*args, **kwds):
            for i in range(100):
                kwds['report_to'](i, 0)
                time.sleep(0.01)
            for i in range(100):
                if i not in progress_list:
                    break
            else:
                assert_flag.value = 1

        cloudfs_types['file'].put = mock.MagicMock(side_effect=put)
        file_path, size, md5_sum = make_file()

        def _on_progress(bytes_completed, size):
            progress_list.append(bytes_completed)

        upload = largetransfer.Upload([file_path], tmp_dir, simple=True)
        upload._on_progress = _on_progress
        upload.apply_async()
        upload.join()

        assert assert_flag.value == 1

    def test_final_progress(self):

        def put(*args, **kwds):
            time.sleep(5)

        file_path, size, md5_sum = make_file(size=10)
        stream = open(file_path, 'rb')
        cloudfs_types['file'].put = mock.MagicMock(side_effect=put)
        progress_cb = mock.MagicMock()
        upload = largetransfer.Upload(
            [stream], tmp_dir, gzip=False, chunk_size=2, progress_cb=progress_cb)
        upload.apply_async()
        upload.join()

        assert upload.progress == 10 * 1024 * 1024, upload.progress
        assert progress_cb.call_count > 2


class TestDownload(object):

    _origin_get = cloudfs_types['file'].get

    def setup(self):
        create_tmp_dir()

    def teardown(self):
        remove_tmp_dir()
        cloudfs_types['file'].get = self._origin_get

    def test__init__default(self):
        download = largetransfer.Download('src')
        assert download.use_pigz is True
        assert download._pool_size == largetransfer.DEFAULT_POOL_SIZE
        assert download._cb_interval == largetransfer.DEFAULT_CALLBACK_INTERVAL
        assert download._manifest is not None
        assert download._callback_thread is None
        assert download._read_fd is None
        assert download._write_fd is None
        assert download.output is None

    def test__init__custom(self):

        def progress_cb(progres):
            return

        download = largetransfer.Download('src', use_pigz=False, pool_size=1,
                                          progress_cb=progress_cb, cb_interval=10)
        assert download.use_pigz is False
        assert download._pool_size == 1
        assert download._cb_interval == 10
        assert download._progress_cb == progress_cb
        assert download._manifest is not None
        assert download._callback_thread is None
        assert download._read_fd is None
        assert download._write_fd is None
        assert download.output is None

    def test_not_running(self):
        download = largetransfer.Download('src')
        assert not download.running

    def test__chunk_generator1(self):
        file_path1, size1, md5_sum1 = make_file(name='chunk.000', size=2)
        file_path2, size2, md5_sum2 = make_file(name='chunk.001', size=2)
        file_path3, size3, md5_sum3 = make_file(name='chunk.002', size=2)
        manifest = largetransfer.Manifest()
        manifest.data = {
            "version": 2.0,
            "description": '',
            "tags": {},
            "files": [
                {
                    'name': 'test_name',
                    'streamer': 'xyz',
                    'compressor': 'zyx',
                    'created_at': None,
                    'chunks': [
                        ('chunk.000', md5_sum1, size1),
                        ('chunk.001', md5_sum2, size2),
                        ('chunk.002', md5_sum3, size3),
                    ]
                }
            ]
        }
        manifest.cloudfs_path = 'file://' + os.path.join(tmp_dir, 'manifest.json')
        manifest.save()

        download = largetransfer.Download(manifest.cloudfs_path)

        i = 0
        try:
            for chunk, streamer, compressor in download._chunk_generator():
                assert os.path.basename(chunk) == 'chunk.00%s' % i, chunk
                assert streamer == 'xyz'
                assert compressor == 'zyx'
                i += 1
        except KeyboardInterrupt:
            assert largetransfer.__thread_error__ is None, largetransfer.__thread_error__

    @mock.patch('scalarizr.storage2.largetransfer.cryptotool')
    def test__chunk_generator2(self, cryptotool_mock):
        file_path1, size1, md5_sum1 = make_file(name='chunk.000', size=2)
        file_path2, size2, md5_sum2 = make_file(name='chunk.001', size=2)
        file_path3, size3, md5_sum3 = make_file(name='chunk.002', size=2)
        manifest = largetransfer.Manifest()
        manifest.data = {
            "version": 2.0,
            "description": '',
            "tags": {},
            "files": [
                {
                    'name': 'test_name',
                    'streamer': 'xyz',
                    'compressor': 'zyx',
                    'created_at': None,
                    'chunks': [
                        ('chunk.000', md5_sum1, size1),
                        ('chunk.001', md5_sum2, size2),
                        ('chunk.002', md5_sum3, size3),
                    ]
                }
            ]
        }
        manifest.cloudfs_path = 'file://' + os.path.join(tmp_dir, 'manifest.json')
        manifest.save()

        cryptotool_mock.calculate_md5_sum.return_value = '000'

        download = largetransfer.Download(manifest.cloudfs_path)

        try:
            for chunk, streamer, compressor in download._chunk_generator():
                pass
        except KeyboardInterrupt:
            assert largetransfer.__thread_error__
            assert largetransfer.__thread_error__[0] == largetransfer.MD5SumError
        except:
            assert False
        else:
            assert False

    def test_apply_async1(self):
        download = largetransfer.Download('src')
        download._run = mock.MagicMock()
        download.apply_async()
        assert download._read_fd is not None
        assert download._write_fd is not None
        assert download.output is not None

    def test_apply_async2(self):
        file_path1, size1, md5_sum1 = make_file(name='chunk.000', size=2)
        file_path2, size2, md5_sum2 = make_file(name='chunk.001', size=2)
        file_path3, size3, md5_sum3 = make_file(name='chunk.002', size=2)
        manifest = largetransfer.Manifest()
        manifest.data = {
            "version": 2.0,
            "description": '',
            "tags": {},
            "files": [
                {
                    'name': 'test_name',
                    'streamer': '',
                    'compressor': '',
                    'created_at': None,
                    'chunks': [
                        ('chunk.000', md5_sum1, size1),
                        ('chunk.001', md5_sum2, size2),
                        ('chunk.002', md5_sum3, size3),
                    ]
                }
            ]
        }
        manifest.cloudfs_path = 'file://' + os.path.join(tmp_dir, 'manifest.json')
        manifest.save()

        download = largetransfer.Download(manifest.cloudfs_path)
        download.apply_async()
        size = 0
        while True:
            data = download.output.read(1024)
            if not data:
                break
            size += len(data)
        download.join()
        assert size == 2 * 3 * 1024 * 1024, size
        assert download.progress == size

    def test_apply_async3(self):
        file_path1, size1, md5_sum1 = make_file(name='chunk.000', size=2)
        file_path2, size2, md5_sum2 = make_file(name='chunk.001', size=2)
        file_path3, size3, md5_sum3 = make_file(name='chunk.002', size=2)
        manifest = largetransfer.Manifest()
        manifest.data = {
            "version": 2.0,
            "description": '',
            "tags": {},
            "files": [
                {
                    'name': 'test_name',
                    'streamer': 'xyz',
                    'compressor': 'zyx',
                    'created_at': None,
                    'chunks': [
                        ('chunk.000', md5_sum1, size1),
                        ('chunk.001', md5_sum2, size2),
                        ('chunk.002', md5_sum3, size3),
                    ]
                }
            ]
        }
        manifest.cloudfs_path = 'file://' + os.path.join(tmp_dir, 'manifest.json')
        manifest.save()

        download = largetransfer.Download(manifest.cloudfs_path)
        download.apply_async()
        try:
            download.join()
        except largetransfer.TransferError as e:
            assert e.args[0] == 'Exception'
            assert e.args[1] == 'Unsupported compressor: zyx'
        except:
            assert False
        else:
            assert False

    def test_stop(self):
        """
        Test stop method when download is not running
        """
        download = largetransfer.Download('src')
        download._kill = mock.MagicMock()
        download.stop()
        assert download._kill.call_count == 0

    def test_terminate(self):
        """
        Test terminate method then download is not running
        """
        download = largetransfer.Download('src', 'dst')
        download._kill = mock.MagicMock()
        download.terminate()
        assert download._kill.call_count == 0, download._kill.call_count

    def test_join(self):
        """
        Test join method then download is not started
        """
        download = largetransfer.Download('src')
        download.join()

    def test_initial_progress(self):
        download = largetransfer.Download('src')
        assert download.progress == 0

    def test_progress(self):
        assert_flag = multiprocessing.Value('i', 0)
        progress_list = []

        def get(*args, **kwds):
            if kwds['report_to']:
                for i in range(100):
                    kwds['report_to'](i, 0)
                    time.sleep(0.01)
                for i in range(100):
                    if i not in progress_list:
                        break
                else:
                    assert_flag.value = 1
            url = args[-2]
            dst = args[-1]
            return os.path.join(dst, os.path.basename(url))

        cloudfs_types['file'].get = mock.MagicMock(side_effect=get)
        file_path1, size1, md5_sum1 = make_file(name='chunk.000', size=2)
        file_path2, size2, md5_sum2 = make_file(name='chunk.001', size=2)
        file_path3, size3, md5_sum3 = make_file(name='chunk.002', size=2)
        manifest = largetransfer.Manifest()
        manifest.data = {
            "version": 2.0,
            "description": '',
            "tags": {},
            "files": [
                {
                    'name': 'test_name',
                    'streamer': '',
                    'compressor': '',
                    'created_at': None,
                    'chunks': [
                        ('chunk.000', md5_sum1, size1),
                        ('chunk.001', md5_sum2, size2),
                        ('chunk.002', md5_sum3, size3),
                    ]
                }
            ]
        }
        manifest.cloudfs_path = 'file://' + os.path.join(tmp_dir, 'manifest.json')
        manifest.save()

        def _on_progress(bytes_completed, size):
            progress_list.append(bytes_completed)

        download = largetransfer.Download(manifest.cloudfs_path)
        download._tmp_dir = tmp_dir
        download._on_progress = _on_progress
        download.apply_async()
        while download.output.read(1024):
            pass
        download.join()

        assert assert_flag.value == 1
