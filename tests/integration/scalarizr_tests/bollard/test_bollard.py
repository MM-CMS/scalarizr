import os
import sys
import signal
import unittest
import sqlite3
import logging
import subprocess
import time

from scalarizr import bollard

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


def create_db():
    conn = sqlite3.connect(DB_PATH)
    try:
        curs = conn.cursor()
        query = (
            """CREATE TABLE IF NOT EXISTS tasks """
            """(task_id TEXT PRIMARY KEY, name TEXT, args TEXT, kwds TEXT, state TEXT, """
            """result TEXT, traceback TEXT, start_date TEXT, end_date TEXT, """
            """worker_id , soft_timeout FLOAT, hard_timeout FLOAT, callbacks TEXT, """
            """meta TEXT) """)
        curs.execute(query)
        conn.commit()
    finally:
        conn.close()


def remove_db():
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)


def clear_db():
    query = ("""DELETE FROM tasks""")
    conn = sqlite3.connect(DB_PATH, timeout=1, isolation_level=None)
    try:
        curs = conn.cursor()
        curs.execute(query)
        conn.commit()
    finally:
        conn.close()


def setUpModule():
    remove_db()
    create_db()


def tearDownModule():
    remove_db()


# XXX
# It's seems Python2.7 has a bug with multiprocessing on Windows
# http://stackoverflow.com/questions/16405687/python-2-7-on-windows-assert-main-name-not-in-sys-modules-main-name-for-all
#
# Traceback (most recent call last):
#   File "<string>", line 1, in <module>
#   File "C:\opt\scalarizr\3.5.0.8195\embedded\python\lib\multiprocessing\forking.py", line 380, in main
#     prepare(preparation_data)
#   File "C:\opt\scalarizr\3.5.0.8195\embedded\python\lib\multiprocessing\forking.py", line 488, in prepare
#     assert main_name not in sys.modules, main_name
# AssertionError: __main__
#
# So we use subprocess to run tests to avoid this error


test_timeout = 60


class TestBollard(unittest.TestCase):

    def setUp(self):
        clear_db()

    def tearDown(self):
        pass

    def _test(self, test_name):
        cmd = [sys.executable, os.path.join(os.path.dirname(__file__), 'subtests.py'), test_name]
        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        for x in range(test_timeout):
            if p.poll() is not None:
                break
            time.sleep(1)
        else:
            p.terminate()
            assert False, '%s timeout' % test_name

        stderr = p.stderr.read()
        assert not stderr, stderr

    def test_start_stop(self):
        self._test('start_stop')

    def test_add_task_by_reference(self):
        self._test('add_by_reference')

    def test_add_task_by_name(self):
        self._test('add_by_name')

    def test_run_task(self):
        self._test('run')

    def test_get_result(self):
        self._test('result')

    def test_stop_with_running_task(self):
        self._test('stop_with_running_task')

    def test_raise_exception(self):
        self._test('raise_exception')

    def test_args_kwds(self):
        self._test('args_kwds')

    def test_revoke_task(self):
        self._test('revoke')

    def test_callback(self):
        self._test('callback')

    def test_soft_timeout(self):
        self._test('soft_timeout')

    def test_hard_timeout(self):
        self._test('hard_timeout')

    def test_get_timeout(self):
        self._test('get_timeout')

    def test_worker_restore(self):
        self._test('worker_restore')

    def test_workers_if_main_crash(self):
        self._test('main_crash')

    def test_restore_after_crash(self):
        self._test('restore_after_crash')

    def test_bind(self):
        self._test('bind')

    def test_current_task(self):
        self._test('current_task')


if __name__ == "__main__":
    unittest.main()
