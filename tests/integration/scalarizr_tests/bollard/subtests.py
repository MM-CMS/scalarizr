import os
import signal
import sys
import logging
import time
import sqlite3
import multiprocessing
import mock

import tasks

from scalarizr import bollard
from scalarizr import util
from scalarizr.util import sqlite_server


prompt = "[%(asctime)s.%(msecs).06d][%(module)20s][%(process)d] %(levelname)10s %(message)s"
frmtr = logging.Formatter(prompt, datefmt='%d/%b/%Y %H:%M:%S')
log_file = os.path.join(os.path.dirname(__file__), 'bollard_test.log')
hndlr = logging.FileHandler(log_file, 'w')
hndlr.setFormatter(frmtr)
bollard.LOG.addHandler(hndlr)
bollard.LOG.setLevel(logging.DEBUG)


if sys.platform == 'win32':
    import win32com.client

    SIGKILL = signal.SIGTERM
else:
    SIGKILL = signal.SIGKILL


DB_PATH = os.path.join(os.path.dirname(__file__), 'test.db')


bollard.bus.cnf = mock.MagicMock()


def connect_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.text_factory = sqlite3.OptimizedUnicode
    return conn


def start_sqlite_server():
    t = sqlite_server.SQLiteServerThread(connect_db)
    t.setDaemon(True)
    t.start()
    sqlite_server.wait_for_server_thread(t)
    bollard.bus.db = t.connection


start_sqlite_server()


bollard.__node__ = mock.MagicMock()
bollard.__node__['platform'].get_access_data = mock.MagicMock(return_value=None)

bollard.bus.periodical_executor = mock.MagicMock()


bollard.CRYPTO_KEY = 'YWZsZztzamRmZ2RscztmZ2pkc2w7ZmpnbDtkamd3O2xlanJnbDtzZGpncnR3ZXI2NDdlZ3NkNGdq\n'


def start_stop():
    executor = bollard.Executor(8)
    executor.start()
    try:
        time.sleep(5)
        assert executor.workers
        for w in executor.workers:
            assert w.is_alive()
            assert w._started_ev.is_set()
            assert w._supervisor_ev.is_set()
        with open(log_file) as f:
            text = f.read()
            assert 'WARNING' not in text
            assert 'ERROR' not in text
    finally:
        executor.stop(workers=True)
        time.sleep(2)
        for w in executor.workers:
            assert not w.is_alive()


def add_by_reference():
    executor = bollard.Executor()
    async_result = executor.apply_async(tasks.foo)

    assert isinstance(async_result, bollard.AsyncResult)
    task = async_result._task
    assert task
    assert task.in_db()
    task.load()
    assert task['state'] == 'pending', task['state']
    assert task['name'] == 'tasks.foo', task['name']


def add_by_name():
    executor = bollard.Executor()
    async_result = executor.apply_async('not bar')

    assert isinstance(async_result, bollard.AsyncResult)
    task = async_result._task
    assert task
    assert task.in_db()
    task.load()
    assert task['state'] == 'pending', task['state']
    assert task['name'] == 'not bar', task['name']


def run():
    executor = bollard.Executor()
    executor.start()
    try:
        async_result = executor.apply_async(tasks.foo)
        time.sleep(2)
        task = async_result._task
        task.load()
        assert task['state'] == 'completed', task['state']
        assert task['start_date'] is not None
        assert task['end_date'] is not None
    finally:
        executor.stop(workers=True)


def result():
    executor = bollard.Executor()
    executor.start()
    try:
        async_result_int = executor.apply_async(tasks.foo_int_result)
        async_result_str = executor.apply_async(tasks.foo_str_result)
        assert async_result_int.get() == 0
        assert async_result_str.get() == '0'
    finally:
        executor.stop(workers=True)


def stop_with_running_task():
    executor = bollard.Executor()
    executor.start()
    try:
        async_result = executor.apply_async(tasks.soft_lock)
        time.sleep(5)
    finally:
        executor.stop(workers=True)
    assert async_result.state == 'running', async_result.state


def raise_exception():
    executor = bollard.Executor(2)
    executor.start()
    try:
        async_result = executor.apply_async(tasks.foo_exc_result)
        time.sleep(5)
        try:
            async_result.get()
        except ZeroDivisionError:
            assert async_result.state == 'failed'
            assert async_result.traceback
            assert async_result.start_date is not None
            assert async_result.end_date is not None
        else:
            assert False
    finally:
        executor.stop(workers=True)


def args_kwds():
    executor = bollard.Executor()
    executor.start()
    try:
        async_result = executor.apply_async(
            tasks.foo_args_kwds, args=(1, 2), kwds={'1': 1, '2': 2})
        assert async_result.get()
    finally:
        executor.stop(workers=True)


def revoke():
    executor = bollard.Executor()
    executor.start()
    try:
        async_result = executor.apply_async(tasks.soft_lock)
        time.sleep(1)
        bollard.Executor.revoke(async_result.task_id)
        try:
            async_result.get()
        except bollard.TaskKilledError:
            assert async_result.state == 'failed', async_result.state
            assert async_result.result
            assert async_result.start_date is not None
            assert async_result.end_date is not None
        else:
            assert False
    finally:
        executor.stop(workers=True)


def soft_timeout():
    executor = bollard.Executor()
    executor.start()
    try:
        async_result = executor.apply_async(tasks.soft_lock, soft_timeout=2, hard_timeout=10)
        try:
            async_result.get()
        except bollard.SoftTimeLimitExceeded:
            assert async_result.state == 'failed', async_result.state
            assert async_result.result
            assert async_result.start_date is not None
            assert async_result.end_date is not None
        except bollard.HardTimeLimitExceeded:
            if sys.platform == 'win32':
                assert True
            else:
                assert False
        else:
            assert False
    finally:
        executor.stop(workers=True)


def hard_timeout():
    executor = bollard.Executor()
    executor.start()
    try:
        async_result = executor.apply_async(tasks.hard_lock, soft_timeout=1, hard_timeout=5)
        try:
            async_result.get()
        except bollard.HardTimeLimitExceeded:
            assert async_result.state == 'failed', async_result.state
            assert async_result.result
            assert async_result.start_date is not None
            assert async_result.end_date is not None
        else:
            assert False
    finally:
        executor.stop(workers=True)


def get_timeout():
    executor = bollard.Executor()
    executor.start()
    try:
        async_result = executor.apply_async(tasks.soft_lock)
        try:
            async_result.get(timeout=1)
        except bollard.TimeoutError:
            assert async_result.state == 'running', async_result.state
            assert async_result.result is None
            assert async_result.start_date is not None
            assert async_result.end_date is None
        else:
            assert False
    finally:
        executor.stop(workers=True)


def worker_restore():
    max_workers = 4
    executor = bollard.Executor(max_workers=max_workers)
    try:
        executor.start()
        time.sleep(2)
        pid = executor.workers[-1].pid
        os.kill(pid, SIGKILL)
        time.sleep(2)
        assert len(executor.workers) == max_workers
        for worker in executor.workers:
            assert worker.is_alive()
    finally:
        executor.stop(workers=True)


# def _task_callback()
class CallbackMock(object):
    def __call__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs


def _callback(task, meta):
    pass


def callback():
    executor = bollard.Executor()
    executor.start()
    try:
        async_result = executor.apply_async(tasks.sleep, args=(0,), callbacks={'task.pull': _callback})
        async_result.get(timeout=1)

        # assert async_result._task in cb.args
    finally:
        executor.stop(workers=True)



def scalarizr_process(max_workers):
    try:
        bollard.SOFT_TIMEOUT = 10
        bollard.HARD_TIMEOUT = 11
        start_sqlite_server()
        executor = bollard.Executor(max_workers)
        executor.start()
        try:
            async_result1 = executor.apply_async(tasks.soft_lock)
            async_result2 = executor.apply_async(tasks.hard_lock)
            async_result1.get()
            async_result2.get()
        finally:
            executor.stop()
    except:
        bollard.LOG.exception(sys.exc_info())


def main_crash():
    max_workers = 4
    scalarizr = multiprocessing.Process(target=scalarizr_process, args=(max_workers,))
    scalarizr.start()
    try:
        while not scalarizr.is_alive():
            time.sleep(1)

        time.sleep(2)

        children = util.get_children(scalarizr.pid)
        assert len(children) == max_workers, children

        os.kill(scalarizr.pid, SIGKILL)

        time.sleep(2)

        assert not scalarizr.is_alive()

        time.sleep(2)

        live_children = [pid for pid in children if bollard.is_alive(pid)]
        assert len(live_children) == 2

        time.sleep(25)

        live_children = [pid for pid in children if bollard.is_alive(pid)]
        assert len(live_children) == 0

    finally:
        try:
            scalarizr.terminate()
        except:
            pass


def scalarizr_process_2():
    try:
        start_sqlite_server()
        executor = bollard.Executor()
        executor.start()
        try:
            async_result = executor.apply_async(tasks.sleep(5))
            async_result.get()
        finally:
            executor.stop()
    except:
        bollard.LOG.exception(sys.exc_info())


def restore_after_crash():
    scalarizr = multiprocessing.Process(target=scalarizr_process_2)
    scalarizr.start()
    try:
        while not scalarizr.is_alive():
            time.sleep(1)

        # check what we have tasks with state 'running'
        conn = connect_db()
        conn.row_factory = sqlite3.Row
        conn.text_factory = sqlite3.OptimizedUnicode
        try:
            curs = conn.cursor()
            query = """SELECT * FROM tasks WHERE state='running'"""
            for i in range(10):
                curs.execute(query)
                data = curs.fetchone()
                if data:
                    break
                time.sleep(0.1)
            else:
                assert False
            task = bollard.Task(**data)
        finally:
            conn.close()

        os.kill(scalarizr.pid, SIGKILL)
        time.sleep(1)
        assert not scalarizr.is_alive()

        task.load()
        assert task['sate'] == 'running'

        executor = bollard.Executor(max_workers=0)
        executor.start()
        time.sleep(10)
        executor.stop()

        task.load()
        assert task['state'] == 'completed', task['state']
    except:
        bollard.LOG.exception(sys.exc_info())


def bind():
    executor = bollard.Executor()
    executor.start()
    try:
        async_result = executor.apply_async('bind')
        try:
            async_result.get(timeout=5)
        except:
            assert False
    finally:
        executor.stop()


def current_task():
    executor = bollard.Executor()
    executor.start()
    try:
        async_result = executor.apply_async('current task')
        result = async_result.get(timeout=5)
        assert result is True, result
    finally:
        executor.stop()


if __name__ == '__main__':
    test_name = sys.argv[1]
    test = getattr(sys.modules['__main__'], test_name)
    test()
