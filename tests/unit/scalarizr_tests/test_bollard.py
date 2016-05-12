import os
import sys
import uuid
import mock
import time
import signal
import sqlite3
import logging
import threading
import unittest

from scalarizr import bollard
from scalarizr.util import sqlite_server

from nose import tools

prompt = "[%(asctime)s.%(msecs).06d][%(module)20s][%(process)d] %(levelname)10s %(message)s"
frmtr = logging.Formatter(prompt, datefmt='%d/%b/%Y %H:%M:%S')
log_file = 'bollard_test.log'
hndlr = logging.FileHandler(log_file, 'w')
hndlr.setFormatter(frmtr)
bollard.LOG.addHandler(hndlr)
bollard.LOG.setLevel(logging.DEBUG)


DB_PATH = os.path.join(os.path.dirname(__file__), 'test.db')


if sys.platform == 'win32':
    SIGKILL = signal.SIGTERM
else:
    SIGKILL = signal.SIGKILL


def connect_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.text_factory = sqlite3.OptimizedUnicode
    return conn


def create_db():
    conn = connect_db()
    try:
        curs = conn.cursor()
        query = (
            """CREATE TABLE IF NOT EXISTS tasks """
            """(task_id TEXT PRIMARY KEY, name TEXT, args TEXT, kwds TEXT, state TEXT, """
            """result TEXT, traceback TEXT, start_date TEXT, end_date TEXT, """
            """worker_id TEXT, soft_timeout FLOAT, hard_timeout FLOAT, callbacks TEXT,"""
            """meta TEXT)""")
        curs.execute(query)
        conn.commit()
    finally:
        conn.close()


def remove_db():
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)


def clear_db():
    query = ("""DELETE FROM tasks""")
    conn = connect_db()
    try:
        curs = conn.cursor()
        curs.execute(query)
        conn.commit()
    finally:
        conn.close()


def setUp():
    remove_db()
    create_db()
    t = sqlite_server.SQLiteServerThread(connect_db)
    t.setDaemon(True)
    t.start()
    sqlite_server.wait_for_server_thread(t)
    bollard.bus.db = t.connection

    bollard.__node__ = mock.MagicMock()
    bollard.__node__['platform'].get_access_data = mock.MagicMock(return_value=None)

    bollard.bus.cnf = mock.MagicMock()
    key = 'YWZsZztzamRmZ2RscztmZ2pkc2w7ZmpnbDtkamd3O2xlanJnbDtzZGpncnR3ZXI2NDdlZ3NkNGdq\n'
    bollard.bus.cnf.read_key.return_value = key


def tearDown():
    try:
        remove_db()
    except WindowsError as e:
        if e.args[0] != 32:
            raise
        import subprocess
        cmd = 'PowerShell -Command "Start-Sleep -s 1; Remove-Item %s"' % DB_PATH
        subprocess.Popen(cmd, shell=True)


class TestTask(object):

    def setUp(self):
        clear_db()

    def tearDown(self):
        pass

    @tools.timed(5)
    def test__init___default(self):
        task = bollard.Task()
        assert task
        assert task['name'] is None, task['name']
        assert task['args'] is None, task['args']
        assert task['kwds'] is None, task['kwds']
        assert task['state'] is None, task['state']
        assert task['result'] is None, task['result']
        assert task['traceback'] is None, task['traceback']
        assert task['start_date'] is None, task['start_date']
        assert task['end_date'] is None, task['end_date']
        assert task['worker_id'] is None, task['worker_id']

    @tools.timed(5)
    def test__init___custom(self):
        task = bollard.Task(task_id='123', name='name', args=(1, 2), kwds={'1': 1, '2': 2},
                             state='pending', result=0, traceback='xxx', start_date='1972',
                             end_date='1972', worker_id='321')
        assert task
        assert task['task_id'] == '123', task['task_id']
        assert task['name'] == 'name', task['name']
        assert task['args'] == (1, 2), task['args']
        assert task['kwds'] == {"1": 1, "2": 2}, task['kwds']
        assert task['state'] == 'pending', task['state']
        assert task['result'] == 0, task['result']
        assert task['traceback'] == 'xxx', task['traceback']
        assert task['start_date'] == '1972', task['start_date']
        assert task['end_date'] == '1972', task['end_date']
        assert task['worker_id'] == '321', task['worker_id']

    @tools.timed(5)
    def test_create_default(self):
        task = bollard.Task.create('test')
        assert task, task
        assert task['task_id'] is not None, task['task_id']
        assert task['name'] == 'test', task['name']
        assert task['args'] == (), task['args']
        assert task['kwds'] == {}, task['kwds']
        assert task['state'] == 'pending', task['state']
        assert task['result'] is None, task['result']
        assert task['traceback'] is None, task['traceback']
        assert task['start_date'] is None, task['start_date']
        assert task['end_date'] is None, task['end_date']
        assert task['worker_id'] is None, task['worker_id']

    @tools.timed(5)
    def test_create_args(self):
        task = bollard.Task.create(uuid.uuid4().hex, args=(1, 2))
        assert task['args'] == (1, 2), task['args']

    @tools.timed(5)
    def test_create_kwds(self):
        task = bollard.Task.create(uuid.uuid4().hex, kwds={'1': 1, '2': 2})
        assert task['kwds'] == {"1": 1, "2": 2}, task['kwds']

    @tools.timed(5)
    def test_insert(self):
        task = bollard.Task.create('test')
        task.insert()

        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        conn.text_factory = sqlite3.OptimizedUnicode
        try:
            curs = conn.cursor()
            query = "SELECT * FROM tasks WHERE task_id='{task_id}'".format(**task)
            curs.execute(query)
            gotten_task = curs.fetchone()
            assert gotten_task
            assert gotten_task['name'] == 'test'
        finally:
            conn.close()

    @tools.timed(5)
    def test_in_db(self):
        task = bollard.Task.create(uuid.uuid4().hex)
        assert not task.in_db()
        task.insert()
        assert task.in_db()

    @tools.timed(5)
    def test_load_tasks(self):
        task = bollard.Task.create(uuid.uuid4().hex)
        task.insert()
        task = bollard.Task.load_by_id(task['task_id'])
        assert task == task

    @tools.timed(5)
    def test_load_by_id(self):
        task = bollard.Task.create(uuid.uuid4().hex)
        task.insert()
        bollard.Task.load_by_id(task['task_id'])
        assert bollard.Task.load_by_id(task['task_id']) == task

    @tools.timed(5)
    def test_delete(self):
        task = bollard.Task.create(uuid.uuid4().hex)
        task.insert()
        bollard.Task.delete(task['task_id'])
        assert not task.in_db()

    @tools.timed(5)
    def test_load(self):
        task = bollard.Task.create(uuid.uuid4().hex)
        task.insert()
        task['state'] == 'completed'
        task.load()
        assert task['state'] == 'pending'

    @tools.timed(5)
    def test_save(self):
        task = bollard.Task.create(uuid.uuid4().hex)
        task.insert()
        task['state'] = 'completed'
        task.save()
        saved_task = bollard.Task.load_by_id(task['task_id'])
        assert saved_task['state'] == 'completed'

    @tools.timed(5)
    def test_reset(self):
        task = bollard.Task.create(uuid.uuid4().hex, args=(1, 2), kwds={'1': 1, '2': 2})
        task['state'] = 'running'
        task['result'] = 0
        task['traceback'] = 'some text here'
        task['start_date'] = '1972'
        task['end_date'] = '1972'
        task['worker_id'] = '123'

        task.reset()

        assert task['state'] == 'pending', task['state']
        assert task['result'] is None, task['result']
        assert task['traceback'] is None, task['traceback']
        assert task['start_date'] is None, task['start_date']
        assert task['end_date'] is None, task['end_date']
        assert task['worker_id'] is None, task['worker_id']

    @tools.timed(5)
    def test_set_exception(self):
        task = bollard.Task.create(uuid.uuid4().hex)
        try:
            1 / 0
        except:
            task.set_exception(sys.exc_info())
        assert task['result']
        assert task['result'] == {
            "exc_message": "integer division or modulo by zero",
            "exc_type": ZeroDivisionError, "exc_data": {}
        }, task['result']


class TestWorker(object):

    @tools.timed(10)
    @mock.patch('multiprocessing.connection.Client')
    def test__get_task(self, mock_client):
        instance = mock_client.return_value
        instance.recv.return_value = ('task')
        m = mock.MagicMock()
        worker = bollard.Worker(m, m, m, m)
        worker._worker_uuid = 'uuid'
        worker._task_lock = threading.Lock()
        worker._popen = mock.MagicMock()
        worker._popen.pid = 111

        worker._get_task()

        assert worker.task == 'task', worker.task
        assert worker._soft_timeout_called is False
        instance.send.assert_called_once_with('111:uuid')
        instance.recv.assert_called_once()
        instance.close.assert_called_once()

    @tools.timed(10)
    @mock.patch('multiprocessing.connection.Client')
    def test__return_result(self, mock_client):
        instance = mock_client.return_value
        m = mock.MagicMock()
        worker = bollard.Worker(m, m, m, m)
        task = bollard.Task(name='task', state='completed')
        worker.task = task

        worker._return_result()

        instance.send.assert_called_once_with(task)
        instance.close.assert_called_once()

    @tools.timed(10)
    @mock.patch('datetime.datetime')
    @mock.patch('os.kill')
    @mock.patch('scalarizr.bollard.is_alive', return_value=True)
    def test__supervisor_ok(self, mock_is_alive, mock_os_kill, mock_datetime):
        m = mock.MagicMock()
        worker = bollard.Worker(m, m, m, m)
        worker.terminate = mock.MagicMock()
        worker._started_ev.set()
        worker._task_lock = mock.MagicMock()
        worker.task = mock.MagicMock()

        t = threading.Thread(target=worker._supervisor)
        t.start()
        time.sleep(4)
        worker._supervisor_ev.clear()
        time.sleep(1)

        assert not t.is_alive()
        assert not worker.terminate.called
        assert not mock_os_kill.called
        assert worker._soft_timeout_called is False

    @tools.timed(10)
    @mock.patch('os.kill')
    @mock.patch('scalarizr.bollard.is_alive', return_value=False)
    def test__supervisor_parent_crash(self, mock_is_alive, mock_os_kill):
        m = mock.MagicMock()
        worker = bollard.Worker(m, m, m, m)
        worker.terminate = mock.MagicMock()
        worker._supervisor_ev.set()
        worker._task_lock = mock.MagicMock()
        worker.task = None

        t = threading.Thread(target=worker._supervisor)
        t.start()
        time.sleep(2)
        worker._supervisor_ev.clear()
        time.sleep(1)

        assert not t.is_alive()
        worker.terminate.assert_called_once()

    @tools.timed(10)
    @mock.patch('datetime.datetime')
    @mock.patch('datetime.timedelta')
    @mock.patch('os.kill')
    @mock.patch('scalarizr.bollard.is_alive', return_value=True)
    def test__supervisor_task_timeout(self, mock_is_alive, mock_os_kill, mock_timedelta, mock_datetime):
        m = mock.MagicMock()
        worker = bollard.Worker(m, m, m, m)
        worker.terminate = mock.MagicMock()
        worker._return_result = mock.MagicMock()
        worker._started_ev.set()
        worker._supervisor_ev.set()
        worker._task_lock = mock.MagicMock()
        worker.task = mock.MagicMock()
        mock_datetime.strptime.return_value = 0
        mock_timedelta.return_value = 0
        mock_datetime.utcnow.return_value = 1

        t = threading.Thread(target=worker._supervisor)
        t.start()
        time.sleep(3)
        worker._started_ev.clear()
        worker._supervisor_ev.clear()
        time.sleep(1)

        assert not t.is_alive()
        worker._return_result.assert_called_once()
        worker.terminate.assert_called_once()

    @tools.timed(10)
    @mock.patch('multiprocessing.Process.start')
    def test_start(self, mock_start):
        m = mock.MagicMock()
        worker = bollard.Worker(m, m, m, m)

        worker.start()

        assert worker.ppid == os.getpid()
        mock_start.assert_called_once()

    @tools.timed(10)
    @mock.patch('scalarizr.util.get_children')
    @mock.patch('os.kill')
    @mock.patch('multiprocessing.Process.start')
    def test_terminate(self, mock_start, mock_os_kill, mock_get_children):
        mock_get_children.return_value = [222222]
        m = mock.MagicMock()
        worker = bollard.Worker(m, m, m, m)
        worker.is_alive = mock.MagicMock(return_value=True)
        worker.join = mock.MagicMock()
        worker._popen = mock.MagicMock()
        worker._popen.pid = 111111

        worker.terminate()

        mock_os_kill.call_count = 2
        mock_os_kill.assert_has_calls(mock.call(111111, bollard.SIGKILL))
        mock_os_kill.assert_has_calls(mock.call(222222, bollard.SIGKILL))
        worker.join.assert_called_once()


class TestAsyncResult(object):

    def setUp(self):
        clear_db()

    def tearDown(self):
        pass

    def test__init__task(self):
        task = bollard.Task.create(uuid.uuid4().hex)
        task.insert()
        assert bollard.AsyncResult(task)

    def test__init__task_id(self):
        task = bollard.Task.create(uuid.uuid4().hex)
        task.insert()
        assert bollard.AsyncResult(task['task_id'])

    def test__getattr__(self):
        task = bollard.Task.create(uuid.uuid4().hex)
        task.insert()
        async_result = bollard.AsyncResult(task)
        assert async_result.name == task['name']
        assert async_result.task_id == task['task_id']
        assert async_result.result is None

    @tools.timed(5)
    def test_get(self):

        def foo(task):
            time.sleep(2)
            task['state'] = 'completed'
            task['result'] = 'Hello World!'
            task.save()

        task = bollard.Task.create(uuid.uuid4().hex)
        task.insert()
        async_result = bollard.AsyncResult(task)

        threading.Thread(target=foo, args=(task,)).start()

        assert async_result.get() == 'Hello World!', async_result.get()

    @tools.timed(5)
    def test_get_timeout(self):
        task = bollard.Task.create(uuid.uuid4().hex)
        task.insert()
        async_result = bollard.AsyncResult(task)
        try:
            async_result.get(timeout=1)
        except bollard.TimeoutError:
            assert True
        else:
            assert False

    @tools.timed(5)
    def test_get_failed(self):

        def foo(task):
            time.sleep(1)
            task['state'] = 'failed'
            try:
                1 / 0
            except:
                task.set_exception(sys.exc_info())
            task.save()

        task = bollard.Task.create(uuid.uuid4().hex)
        task.insert()
        async_result = bollard.AsyncResult(task)

        threading.Thread(target=foo, args=(task,)).start()

        try:
            async_result.get()
        except ZeroDivisionError:
            assert True
        else:
            assert False

    @tools.timed(5)
    def test_get_by_ready_event(self):

        def foo(task):
            task['state'] = 'completed'
            task['result'] = 'Hello World!'
            task.save()
            bollard.Executor.ready_events[task['task_id']].set()

        task = bollard.Task.create(uuid.uuid4().hex)
        task.insert()
        async_result = bollard.AsyncResult(task)
        bollard.Executor.ready_events[task['task_id']] = threading.Event()

        threading.Thread(target=foo, args=(task,)).start()

        assert async_result.get() == 'Hello World!'

    @tools.timed(10)
    def test_get_ready_event_failed(self):

        def foo(task):
            task['state'] = 'completed'
            task['result'] = 'Hello World!'
            task.save()

        task = bollard.Task.create(uuid.uuid4().hex)
        task.insert()
        async_result = bollard.AsyncResult(task)
        bollard.Executor.ready_events[task['task_id']] = threading.Event()

        threading.Thread(target=foo, args=(task,)).start()

        assert async_result.get() == 'Hello World!'


def test_is_alive_true():
    data = (
            """Name:\tnfsiod\nState:\tS (sleeping)\nTgid:\t768\nNgid:\t0\nPid:\t768\n"""
            """PPid:\t2\nTracerPid:\t0\nUid:\t0\t0\t0\t0\nGid:\t0\t0\t0\t0\nFDSize:\t64\n"""
            """Groups:\t\nThreads:\t1\nSigQ:\t0/7874\nSigPnd:\t0000000000000000\n"""
            """ShdPnd:\t0000000000000000\nSigBlk:\t0000000000000000\nSigIgn:\tffffffffffffffff\n"""
            """SigCgt:\t0000000000000000\nCapInh:\t0000000000000000\nCapPrm:\t0000001fffffffff\n"""
            """CapEff:\t0000001fffffffff\nCapBnd:\t0000001fffffffff\nSeccomp:\t0\n"""
            """Cpus_allowed:\t1\nCpus_allowed_list:\t0\nMems_allowed:\t00000000,00000001\n"""
            """Mems_allowed_list:\t0\nvoluntary_ctxt_switches:\t2\n"""
            """nonvoluntary_ctxt_switches:\t0\n'"""
    )
    with mock.patch('__builtin__.open', mock.mock_open(read_data=data), create=True) as m:
        assert bollard.is_alive(os.getpid())


def test_is_alive_false():
    data = (
            """Name:\tnfsiod\nState:\tZ (zombie)\nTgid:\t768\nNgid:\t0\nPid:\t768\n"""
            """PPid:\t2\nTracerPid:\t0\nUid:\t0\t0\t0\t0\nGid:\t0\t0\t0\t0\nFDSize:\t64\n"""
            """Groups:\t\nThreads:\t1\nSigQ:\t0/7874\nSigPnd:\t0000000000000000\n"""
            """ShdPnd:\t0000000000000000\nSigBlk:\t0000000000000000\nSigIgn:\tffffffffffffffff\n"""
            """SigCgt:\t0000000000000000\nCapInh:\t0000000000000000\nCapPrm:\t0000001fffffffff\n"""
            """CapEff:\t0000001fffffffff\nCapBnd:\t0000001fffffffff\nSeccomp:\t0\n"""
            """Cpus_allowed:\t1\nCpus_allowed_list:\t0\nMems_allowed:\t00000000,00000001\n"""
            """Mems_allowed_list:\t0\nvoluntary_ctxt_switches:\t2\n"""
            """nonvoluntary_ctxt_switches:\t0\n'"""
    )
    with mock.patch('__builtin__.open', mock.mock_open(read_data=data), create=True) as m:
        assert not bollard.is_alive(-111)


if __name__ == "__main__":
    unittest.main()
