import os
import copy
import datetime
import json
import logging
import multiprocessing
import multiprocessing.connection
import multiprocessing.synchronize
import pickle
import Queue
import re
import signal
import socket
import sys
import threading
import time
import traceback
import types
import uuid
import pprint


from abc import ABCMeta, abstractmethod

from scalarizr import util
from scalarizr.bus import bus
from scalarizr.node import __node__
from scalarizr.libs import bases
from scalarizr.util import cryptotool

if sys.platform == 'win32':
    import win32api
    import win32con
    import win32job
    import win32process

    push_server_base_address = r'\\.\pipe'
    pull_server_base_address = r'\\.\pipe'
    SIGKILL = signal.SIGTERM
else:
    from ctypes import cdll

    push_server_base_address = '/tmp'
    pull_server_base_address = '/tmp'
    SIGKILL = signal.SIGKILL

LOG = logging.getLogger(__name__)

MAX_WORKERS = 4
# Default soft/hard timeouts are infinite (max 32bit C int)
SOFT_TIMEOUT = 21474836471 - 60
HARD_TIMEOUT = 21474836471
SEND_RESULT_TIMEOUT = 60
ERROR_SLEEP = 5
EXECUTOR_POLL_SLEEP = 1
WORKER_SUPERVISOR_SLEEP = 1
TASKS_TTL = 48 * 3600

CRYPTO_KEY = None
AUTH_KEY = None


__tasks__ = {}


# fix
# AttributeError: _strptime
datetime.datetime.strptime('1970-01-01 00:00:00', '%Y-%m-%d %H:%M:%S')


class BollardError(Exception):
    pass


class TimeoutError(BollardError):
    pass


class SoftTimeLimitExceeded(BollardError):
    pass


class HardTimeLimitExceeded(BollardError):
    pass


class TaskKilledError(BollardError):
    pass


class AlreadyInProgressError(BollardError):
    pass


class Event(multiprocessing.synchronize.Event):

    """Custom Event class to avoid lock in multiprocessing.Event.wait() method"""

    def wait(self, timeout=None):
        if timeout:
            result = super(Event, self).wait(timeout=timeout)
        else:
            while True:
                result = super(Event, self).wait(timeout=1)
                if result:
                    break
        return result


class CallbackCenter(bases.Observable):
    """
    Class for launching callbacks after certain events in Bollard.

    callback_scheme example:
    {
        'global.push': func1,
        'global.pull': [func2, func3],
        'task.before': [func4, func5],
        'task.after': func6
    }
    funcX objects must be callable.
    """

    def __init__(self, callback_scheme=None):
        super(CallbackCenter, self).__init__(
                'global.push', 'global.pull', 'global.before', 'global.after', 'global.fork',
                'task.push', 'task.pull', 'task.before', 'task.after')
        callback_scheme = callback_scheme or {}

        for k, v in callback_scheme.items():
            if not hasattr(v, '__iter__'):
                v = [v]
            self.on(k, *v)

    def fire(self, event, args=None, kwargs=None, raise_exc=False):
        if self._events_suspended:
            LOG.debug('Trying to launch callbacks for %s, but callbacks are suspended', event)
            return

        args = args or ()
        kwargs = kwargs or {}
        callbacks = self._listeners.get(event, ())

        if callbacks:
            LOG.debug('Executing {!r} callbacks...'.format(event))

        for callback in callbacks:
            callback_name = 'unnamed callback'
            if hasattr(callback, '__name__'):
                callback_name = callback.__name__
            elif hasattr(callback, 'func'):
                callback_name = callback.func.__name__
            LOG.debug('Executing {!r} callback {!r}'.format(event, callback_name))
            try:
                callback(*args, **kwargs)
            except:
                if raise_exc:
                    raise
                else:
                    LOG.exception(
                        'Failed {!r} callback {!r}'.format(event, callback_name),
                        exc_info=sys.exc_info())
        if callbacks:
            LOG.debug('Finished {!r} callbacks'.format(event))


def get_connection():
    return bus.db


_current_task = None


def current_task():
    return _current_task


def tasks_cleanup():
    ttl = __node__['base']['keep_scripting_logs_time'] or TASKS_TTL
    dtime = datetime.datetime.utcnow() - datetime.timedelta(seconds=ttl)
    dtime = dtime.replace(microsecond=0)
    query = (
        """DELETE FROM tasks """
        """WHERE start_date<'{dtime}'"""
    ).format(dtime=dtime)
    conn = get_connection()
    curs = conn.cursor()
    curs.execute(query)


class Task(dict):

    schema = (
        'task_id',
        'name',
        'args',
        'kwds',
        'state',
        'result',
        'traceback',
        'start_date',
        'end_date',
        'worker_id',
        'soft_timeout',
        'hard_timeout',
        'callbacks',
        'meta',
    )

    json_items = ('args', 'kwds')
    pickled_items = ('result', 'meta', 'callbacks')
    ciphered_items = ('args', 'kwds', 'result', 'callbacks', 'meta')

    @classmethod
    def _load(cls, *args, **kwds):
        """
        :param args: field names to select
        :type args: tuple
        :param kwds: condition for select statement
        :type kwds: dict
        :returns: row as dict from database
        :rtype: list
        """

        args = args or cls.schema
        names = ', '.join(args)
        query = "SELECT {} FROM tasks".format(names)
        if kwds:
            where = True
            for k, v in kwds.iteritems():
                if where:
                    query += " WHERE {}=\"{}\"".format(k, v)
                    where = False
                else:
                    query += " AND {}=\"{}\"".format(k, v)
        conn = get_connection()
        curs = conn.cursor()
        curs.execute(query)
        for result in curs.fetchall():
            data = dict(result)
            deserialized = dict((k, cls._deserialize(k, v)) for k, v in data.iteritems())
            yield deserialized

    @classmethod
    def load_by_id(cls, task_id):
        """
        Load task from database by task id
        :returns: Task object
        :rtype: Task
        """

        for data in cls._load(task_id=task_id):
            return cls(**data)

    @classmethod
    def load_tasks(cls, *args, **kwds):
        """
        Load fields listed in args according to condition in kwds from database
        :param args: field names to select
        :type args: tuple
        :param kwds: condition for select statement
        :type kwds: dict
        :returns: Task object
        :rtype: generator
        """

        for data in cls._load(*args, **kwds):
            yield cls(**data)

    @classmethod
    def create(cls,
        name,
        args=None,
        kwds=None,
        soft_timeout=None,
        hard_timeout=None,
        callbacks=None):
        """
        Create Task object
        :returns: Task object
        :rtype: Task
        """

        data = {
            'task_id': uuid.uuid4().hex,
            'name': name,
            'args': args or (),
            'kwds': kwds or {},
            'state': 'pending',
            'soft_timeout': soft_timeout or SOFT_TIMEOUT,
            'hard_timeout': hard_timeout or HARD_TIMEOUT,
            'callbacks': callbacks or {},
            'meta': {'persistent': {}, 'ephemeral': {}}
        }
        return cls(**data)

    def __init__(self, **kwds):
        super(Task, self).__init__()
        for k in self.schema:
            v = kwds.get(k)
            super(Task, self).__setitem__(k, v)

        self.request = self  # Celery compatibility

    @classmethod
    def _deserialize(cls, key, value):
        if CRYPTO_KEY and key in cls.ciphered_items:
            value = cryptotool.decrypt_aes256(value, CRYPTO_KEY)
        if key in cls.json_items:
            serializer = json.loads
        elif key in cls.pickled_items:
            serializer = pickle.loads
        else:
            serializer = lambda item: item

        if key == 'meta':
            value = {'persistent': serializer(value), 'ephemeral': {}}
        elif key == 'args':
            value = tuple(serializer(value))
        elif key in ('start_date', 'end_date') and isinstance(value, basestring):
            value = datetime.datetime.strptime(value, '%Y-%m-%d %H:%M:%S')
        else:
            value = serializer(value)
        return value

    @classmethod
    def delete(cls, task_id):
        """Delete task from database"""

        query = "DELETE FROM tasks WHERE task_id=?"
        conn = get_connection()
        curs = conn.cursor()
        curs.execute(query, (task_id,))

    def to_logging_format(self):
        """ Hide ciphered fields for security reasons """
        tmp = self.copy()
        for key in self.ciphered_items:
            tmp[key] = '***'
        return tmp

    def __repr__(self):
        return '{}({})'.format(self.name, self.id)

    def prepare_to_pickle(self):
        if self['state'] == 'failed':
            self['traceback'] = ''.join(traceback.format_tb(self['traceback']))
            try:
                pickle.dumps(self['result']['exc_type'])
            except pickle.PicklingError:
                self['result']['exc_message'] = '{}: {}'.format(
                        self['result']['exc_type'].__name__, self['result']['exc_message'])
                self['result']['exc_type'] = BollardError

    def _serialize(self, key):
        if key == 'result' and self['state'] == 'failed':
            try:
                value = pickle.dumps(self[key])
            except pickle.PicklingError:
                dupl = copy(self[key])
                dupl['exc_message'] = '{}: {}'.format(dupl['exc_type'].__name__, dupl['exc_message'])
                dupl['exc_type'] = BollardError
                value = pickle.dumps(dupl)
        elif key == 'meta':
            value = pickle.dumps(self['meta']['persistent'])
        elif key == 'traceback' and isinstance(self[key], types.TracebackType):
            value = ''.join(traceback.format_tb(self[key]))
        elif key in ('start_date', 'end_date') and isinstance(self[key], datetime.datetime):
            value = self[key].strftime('%Y-%m-%d %H:%M:%S')
        elif key in self.json_items:
            value = json.dumps(self[key])
        elif key in self.pickled_items:
            value = pickle.dumps(self[key])
        else:
            value = self[key]
        if CRYPTO_KEY and key in self.ciphered_items:
            value = cryptotool.encrypt_aes256(value, CRYPTO_KEY)
        return value

    def in_db(self):
        """
        :returns: is task in database
        :rtype: bool
        """

        for _ in self._load('task_id', task_id=self._serialize('task_id')):
            return True
        return False

    def reset(self):
        """
        Reset task to pending state and save into database
        Use tmp object for thread safety
        """

        tmp = {}
        tmp['state'] = 'pending'
        tmp['result'] = None
        tmp['traceback'] = None
        tmp['start_date'] = None
        tmp['end_date'] = None
        tmp['worker_id'] = None
        tmp['meta'] = {'persistent': {}, 'ephemeral': {}}
        self.update(tmp)

    def acquire(self, worker_id):
        """
        Acquire task
        Use tmp object for thread safety
        """

        self.load()
        tmp = copy.copy(self)
        if tmp['state'] != 'pending':
            return False
        tmp['state'] = 'running'
        tmp['worker_id'] = worker_id
        tmp['start_date'] = datetime.datetime.utcnow()
        tmp.save()
        self.update(tmp)
        return True

    def insert(self):
        """Insert task into database"""

        query = (
            """INSERT OR IGNORE INTO tasks VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) """
        )
        conn = get_connection()
        curs = conn.cursor()
        values = tuple((self._serialize(key) for key in self.schema))
        curs.execute(query, values)

    def load(self, *args):
        """Update local object from database"""

        for data in self._load(*args, task_id=self['task_id']):
            data['meta']['ephemeral'] = self['meta']['ephemeral']
            self.update(data)
            return

    def save(self, *args):
        """
        Update database from local object
        :param args: field names to save
        :type args: tuple
        """

        args = args or tuple(self.schema)
        query = "UPDATE tasks SET "
        for name in args:
            query += '%s=?, ' % name
        query = query[:-2] + ' WHERE task_id=?'
        values = tuple(self._serialize(name) for name in args) + (self['task_id'],)
        conn = get_connection()
        curs = conn.cursor()
        curs.execute(query, values)

    def set_result(self, result):
        self['result'] = result
        self['state'] = 'completed'

    def set_exception(self, exc_info):
        self['result'] = {
            'exc_type': exc_info[0],
            'exc_message': str(exc_info[1]),
            'exc_data': exc_info[1].__dict__,
        }
        self['end_date'] = datetime.datetime.utcnow()
        self['traceback'] = exc_info[2]
        self['state'] = 'failed'

    @property
    def result(self):
        return self['result']

    @property
    def exception(self):
        if self['state'] == 'failed':
            result = self['result']
            return result['exc_type'](result['exc_message'], **result['exc_data'])
        return None

    def scalr_serialize(self):
        state_map = {
            'pending': 'new',
            'running': 'in-progress',
            'completed': 'completed',
            'failed': 'failed'
        }
        ret = {
            'id': self['task_id'],
            'name': self['name'],
            'status': state_map[self['state']],
            'result': self.result if self['state'] == 'completed' else None,
            'error': self.result['exc_message'] if self['state'] == 'failed' else None,
            'trace': self['traceback'],
            'logs': [],
            'start_date': self['start_date'] and self['start_date'].isoformat() or None,
            'finish_date': self['end_date'] and self['end_date'].isoformat() or None,
        }
        return ret

    def __call__(self, *args, **kwds):
        func = __tasks__[self['name']]['func']
        bind = __tasks__[self['name']]['bind']
        if bind:
            args = (self,) + args
        return func(*args, **kwds)

    @property
    def id(self):
        return self['task_id']

    @property
    def name(self):
        return self['name']

    @property
    def func(self):
        return __tasks__[self['name']]['func']


def task(func=None, name=None, bind=False, base=Task, exclusive=False, notify=True):
    """Task decorator"""
    if func is not None:
        assert isinstance(func, types.FunctionType)
        if name is None:
            name = '%s.%s' % (func.__module__, func.__name__)
        assert name not in __tasks__, 'Task with name %s already exists' % name
        func.local = threading.local()
        __tasks__.update({
            name: {
                'func': func,
                'bind': bind,
                'base': base,
                'exclusive': exclusive,
                'notify': notify,
            }})

        def bollard_task(*args, **kwds):
            global _current_task
            if hasattr(func.local, 'worker_id') and func.local.worker_id:
                func.local.worker_id = None
                return func(*args, **kwds)
            else:
                _current_task = base.create(name)
                try:
                    return _current_task(*args, **kwds)
                finally:
                    _current_task = None

        bollard_task.name = name
        return bollard_task

    else:

        def wrapper(func):
            return task(func, name=name, bind=bind, base=base, exclusive=exclusive)

        return wrapper


class IPCServer(threading.Thread):

    __metaclass__ = ABCMeta

    def __init__(self, address, authkey, callback_launcher):
        self.address = address
        self.authkey = authkey
        self.listener = None
        self.cls_name = '{}.{}'.format(self.__module__, self.__class__.__name__)
        self._callback_launcher = callback_launcher
        self._terminate = threading.Event()
        super(IPCServer, self).__init__()
        self.daemon = True

    def run(self):
        LOG.debug('{} started'.format(self.cls_name))

        while not self._terminate.is_set():
            try:
                if sys.platform != 'win32' and os.path.exists(self.address):
                    LOG.debug('Removing {}'.format(self.address))
                    os.remove(self.address)

                self.listener = multiprocessing.connection.Listener(self.address,
                                                                    authkey=self.authkey)
                try:
                    while not self._terminate.is_set():
                        conn = self.listener.accept()
                        try:
                            self.handle(conn)
                        except:
                            msg = '{} handle error: {}'
                            msg = msg.format(self.cls_name, sys.exc_info()[:2])
                            LOG.exception(msg)
                        finally:
                            conn.close()
                finally:
                    self.listener.close()
            except multiprocessing.AuthenticationError:
                LOG.error(sys.exc_info()[0:2])
            except:
                if isinstance(sys.exc_info()[1], KeyboardInterrupt) or self._terminate.is_set():
                    break
                else:
                    LOG.exception('{} error: {}'.format(self.cls_name, sys.exc_info()[:2]))
                    time.sleep(ERROR_SLEEP)

        if sys.platform != 'win32' and os.path.exists(self.address):
            os.remove(self.address)
        LOG.debug('{} exited'.format(self.cls_name))

    def terminate(self):
        """Function naming same as multiprocessing.Process.terminate"""

        try:
            self._terminate.set()
            if sys.platform == 'win32':
                try:
                    conn = multiprocessing.connection.Client(self.address, authkey=self.authkey)
                except WindowsError:
                    pass
            else:
                self.listener._listener._socket.shutdown(socket.SHUT_RDWR)
        except:
            LOG.exception('{} terminate error: {}'.format(self.cls_name, sys.exc_info()[:2]))

    @abstractmethod
    def handle(self, conn):
        return


class PushServer(IPCServer):

    def __init__(self, queue, *args, **kwds):
        super(PushServer, self).__init__(*args, **kwds)
        self.queue = queue

    def handle(self, conn):
        worker_id = conn.recv()
        while not self._terminate.is_set():
            try:
                task = self.queue.get(timeout=1)
            except Queue.Empty:
                continue
            if not task.acquire(worker_id):
                continue
            self._callback_launcher.fire('global.push', (task, task['meta']))
            CallbackCenter(task['callbacks']).fire('task.push', (task, task['meta']))
            task.save('meta')  # save persistent meta
            LOG.info('Pushed task {!r}'.format(task))
            LOG.debug('(debug)Pushed task {!r}\n{}'.format(
                task, pprint.pformat(task.to_logging_format())))
            conn.send(task)
            return


class PullServer(IPCServer):

    def handle(self, conn):
        task = conn.recv()
        if task['state'] in ('completed', 'failed'):
            task['end_date'] = datetime.datetime.utcnow()
        task.save()
        LOG.info('Pulled task {!r} result'.format(task))
        LOG.debug('(debug)Pulled task {!r} result\n{}'.format(
            task, pprint.pformat(task.result)))
        CallbackCenter(task['callbacks']).fire('task.pull', (task, task['meta']))
        self._callback_launcher.fire('global.pull', (task, task['meta']))
        try:
            Executor.ready_events[task['task_id']].set()
            del Executor.ready_events[task['task_id']]
        except KeyError:
            pass


class Executor(object):

    _workers = {}
    ready_events = {}

    def __init__(self,
            max_workers=None,
            soft_timeout=None,
            hard_timeout=None,
            task_modules=None,
            pull_server_address=None,
            push_server_address=None,
            callbacks=None):
        self._push_queue = Queue.Queue()
        self._push_server_address = push_server_address or os.path.join(push_server_base_address,
                'szr_{}.sock'.format(uuid.uuid4().hex))
        self._pull_server_address = pull_server_address or os.path.join(pull_server_base_address,
                'szr_{}.sock'.format(uuid.uuid4().hex))
        self._ipc_authkey = AUTH_KEY
        self._push_server = None
        self._pull_server = None
        self._max_workers = max_workers or MAX_WORKERS
        self._soft_timeout = soft_timeout or SOFT_TIMEOUT
        self._hard_timeout = hard_timeout or HARD_TIMEOUT
        self._state = 'stopped'
        self._state_lock = threading.Lock()
        self._poll_thread = None
        self._callback_launcher = CallbackCenter(callbacks)

        task_modules = task_modules or ()
        for module in task_modules:
            __import__(module)

        # windows only
        self._job_obj = None

    @property
    def workers(self):
        return self.__class__._workers.get(self, [])

    @workers.setter
    def workers(self, value):
        self.__class__._workers[self] = value

    def _launch_worker(self, wait=False):
        worker = Worker(self._push_server_address,
            self._pull_server_address,
            self._ipc_authkey,
            self._callback_launcher)
        worker.start()

        if sys.platform == 'win32':
            permissions = win32con.PROCESS_TERMINATE | win32con.PROCESS_SET_QUOTA
            handle = win32api.OpenProcess(permissions, False, worker.pid)
            try:
                win32job.AssignProcessToJobObject(self._job_obj, handle)
            finally:
                win32api.CloseHandle(handle)

        if wait:
            worker.wait_start()
        self.workers.append(worker)
        return worker

    def _check_workers(self):
        self.workers = [worker for worker in self.workers if worker.is_alive()]
        for _ in xrange(self._max_workers - len(self.workers)):
            self._launch_worker()

    def _validate_running_tasks(self):
        workers_ids = [worker.worker_id for worker in self.workers if worker.is_alive()]
        for task in Task.load_tasks(state='running'):
            if str(task['worker_id']) in workers_ids:
                continue
            worker_pid = task['worker_id'].split(':')[0]
            if is_alive(worker_pid):
                continue
            LOG.debug('Found invalid task {}, reset it'.format(task.to_logging_format()))
            task_cls = __tasks__[task['name']]['base']
            task = task_cls.load_by_id(task['task_id'])
            task.reset()
            task.save()
            if self._state == 'started':
                self._push_queue.put(task)

    def _poll(self):
        #Separate thread

        while True:
            try:
                with self._state_lock:
                    # use lock to avoid simultaneously starting and stopping workers from different
                    # threads
                    if self._state == 'stopped':
                        return
                    self._check_workers()

                self._validate_running_tasks()

                time.sleep(EXECUTOR_POLL_SLEEP)
            except:
                LOG.exception('Executor poll error: {}'.format(sys.exc_info()[:2]))
                time.sleep(ERROR_SLEEP)

    def start(self):
        if self._state != 'stopped':
            return

        LOG.debug('Starting Executor')

        self._state = 'starting'

        self._push_server = PushServer(self._push_queue,
            self._push_server_address,
            self._ipc_authkey,
            self._callback_launcher)
        self._pull_server = PullServer(self._pull_server_address,
            self._ipc_authkey,
            self._callback_launcher)
        self._push_server.start()
        self._pull_server.start()

        if sys.platform == 'win32':
            self._job_obj = win32job.CreateJobObject(None, 'Bollard')
            ex_info = win32job.QueryInformationJobObject(self._job_obj,
                win32job.JobObjectExtendedLimitInformation)
            ex_info['BasicLimitInformation']['LimitFlags'] = \
                win32job.JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
            win32job.SetInformationJobObject(self._job_obj,
                win32job.JobObjectExtendedLimitInformation,
                ex_info)

        self._validate_running_tasks()

        for task in Task.load_tasks(state='pending'):
            self._push_queue.put(task)

        # wait for push and pull server before start workers
        while not self._push_server.is_alive() or not self._pull_server.is_alive():
            time.sleep(0.1)

        self._poll_thread = threading.Thread(target=self._poll)
        self._poll_thread.daemon = True
        self._poll_thread.start()

        __node__['periodical_executor'].add_task(tasks_cleanup, 3600, title='Bollard cleanup')

        self._state = 'started'
        LOG.debug('Executor started')

    def stop(self, workers=True):
        if self._state != 'started':
            return

        LOG.debug('Stopping Executor')

        with self._state_lock:
            self._state = 'stopped'
            if workers:
                # use lock to avoid simultaneously starting and stopping workers from different
                # threads
                for worker in self.workers:
                    worker.terminate()
                self.workers = []

        self._push_server.terminate()
        self._pull_server.terminate()

        while self._push_server.is_alive() or self._pull_server.is_alive():
            time.sleep(0.1)
        while self._poll_thread.is_alive():
            time.sleep(0.1)

        if __node__['periodical_executor']:
            __node__['periodical_executor'].remove_task(tasks_cleanup)

        LOG.debug('Executor stopped')

    def apply_async(self,
        task_name,
        args=None,
        kwds=None,
        soft_timeout=None,
        hard_timeout=None,
        callbacks=None):

        if isinstance(task_name, types.FunctionType):
            task_name = task_name.name

        assert task_name in __tasks__, "Task '{}' not found in registered tasks".format(task_name)

        if __tasks__[task_name]['exclusive']:
            for state in ('pending', 'running'):
                for task in Task.load_tasks(name=task_name, state=state):
                    msg = "'{}' already in progress".format(task['name'])
                    raise AlreadyInProgressError(msg, task['task_id'])

        soft_timeout = soft_timeout or self._soft_timeout
        hard_timeout = hard_timeout or self._hard_timeout

        task_cls = __tasks__[task_name]['base']
        task = task_cls.create(task_name,
            args=args,
            kwds=kwds,
            soft_timeout=soft_timeout,
            hard_timeout=hard_timeout,
            callbacks=callbacks)
        LOG.debug('Apply task {}'.format(task.to_logging_format()))
        task.insert()

        if self._state == 'started':
            self._push_queue.put(task)

        self.__class__.ready_events[task['task_id']] = threading.Event()
        return AsyncResult(task)

    @classmethod
    def revoke(cls, task_id):
        """
        Only running tasks can be revoked
        """
        for task in Task.load_tasks(task_id=task_id):
            if task['state'] != 'running':
                continue
            try:
                LOG.debug('Killing task {}'.format(task_id))
                raise TaskKilledError()
            except:
                task.set_exception(sys.exc_info())
                task.save()
            finally:
                for _, workers in Executor._workers.iteritems():
                    for worker in workers:
                        worker.lock()
                        try:
                            if str(task['worker_id']) == worker.worker_id:
                                worker.terminate()
                                break
                        finally:
                            worker.unlock()


prog = re.compile(r'.*\nState:\t*(.) *\((.*)\)\n.*')
def is_alive(pid):
    pid = int(pid)
    if sys.platform == 'win32':
        if pid not in win32process.EnumProcesses():
            return False
    else:
        try:
            with open('/proc/%d/status' % pid, 'r') as f:
                text = f.read()
                match = prog.match(text)
                assert match.groups()[0] != 'Z'
        except (IOError, AttributeError, AssertionError):
            return False
    return True


class Worker(multiprocessing.Process):

    terminate_timeout = 10

    def __init__(self, push_server_addr, pull_server_addr, authkey, callback_launcher):
        self._push_server_address = push_server_addr
        self._pull_server_address = pull_server_addr
        self._ipc_authkey = authkey
        self._callback_launcher = callback_launcher
        self._started_ev = Event()
        self._supervisor_ev = Event()
        self._lock_ev = Event()
        self._task_lock = None
        self._worker_uuid = None
        self._ppid = None
        self._soft_timeout_called = False
        self._terminate_time = None

        self.task = None

        super(Worker, self).__init__()

    @property
    def worker_id(self):
        if self.pid:
            return '{}:{}'.format(self.pid, self._worker_uuid)
        else:
            return None

    @property
    def ppid(self):
        return self._ppid

    def lock(self):
        """Lock worker to prevent getting new task"""

        self._lock_ev.clear()

    def unlock(self):
        """Unlock worker to allow getting new task"""

        self._lock_ev.set()

    def _signal_handler(self, signum, frame):
        if signum == signal.SIGUSR1:
            raise SoftTimeLimitExceeded()

    def _get_task(self):
        attempts = 5
        while attempts:
            try:
                conn = multiprocessing.connection.Client(self._push_server_address,
                                                         authkey=self._ipc_authkey)
                try:
                    conn.send(self.worker_id)
                    self.task = conn.recv()
                finally:
                    conn.close()

                self._soft_timeout_called = False
                break
            except (socket.error, EOFError, IOError):
                attempts -= 1
                msg = 'Worker {} communication error: {}'.format(self.worker_id, sys.exc_info()[:2])
                LOG.debug(msg)
                time.sleep(ERROR_SLEEP)
        else:
            raise

    def _return_result(self):
        self.task.prepare_to_pickle()

        def send():
            while True:
                try:
                    conn = multiprocessing.connection.Client(self._pull_server_address,
                            authkey=self._ipc_authkey)
                    try:
                        conn.send(self.task)
                        break
                    finally:
                        conn.close()
                except:
                    msg = 'Worker {} error: unable to send result, reason: {}'
                    msg = msg.format(self.worker_id, sys.exc_info()[0:2])
                    LOG.debug(msg)
                    time.sleep(ERROR_SLEEP)

        t = threading.Thread(target=send)
        t.start()
        t.join(SEND_RESULT_TIMEOUT)
        if t.is_alive():
            msg = 'Worker {} error: unable to send result, timeout'.format(self.worker_id)
            LOG.error(msg)
            self.terminate()

    def _check_parent_process(self):
        if is_alive(self.ppid):
            return
        if self.task:
            self._started_ev.clear()
        else:
            msg = 'Worker {} supervisor: parent process is dead'.format(self.worker_id)
            LOG.debug(msg)
            self.terminate()

    def _check_task_timeouts(self):
        self._task_lock.acquire()
        try:
            if self.task:
                # hard timeout
                hard_delta = datetime.timedelta(seconds=float(self.task['hard_timeout']))
                if datetime.datetime.utcnow() > self.task['start_date'] + hard_delta:
                    raise HardTimeLimitExceeded()

                # soft timeout
                # only for linux
                if sys.platform == 'win32':
                    return
                soft_delta = datetime.timedelta(seconds=float(self.task['soft_timeout']))
                if datetime.datetime.utcnow() > self.task['start_date'] + soft_delta:
                    if self._soft_timeout_called:
                        return
                    raise SoftTimeLimitExceeded()

        except HardTimeLimitExceeded:
            msg = 'Hard timeout has been occurred for task {}'.format(self.task['task_id'])
            LOG.warning(msg)
            self.task.set_exception(sys.exc_info())
            self._return_result()
            self.terminate()
        except SoftTimeLimitExceeded:
            msg = 'Soft timeout has been occurred for task {}'.format(self.task['task_id'])
            LOG.warning(msg)
            os.kill(self.pid, signal.SIGUSR1)
            self._soft_timeout_called = True
        finally:
            self._task_lock.release()

    def _supervisor(self):
        self._supervisor_ev.set()
        while self._supervisor_ev.is_set():
            try:
                self._check_parent_process()
                self._check_task_timeouts()
            except:
                msg = 'Worker {} supervisor error: {}'.format(self.worker_id, sys.exc_info()[:2])
                LOG.exception(msg)
                self.terminate()
            finally:
                time.sleep(WORKER_SUPERVISOR_SLEEP)
        self._supervisor_ev.clear()

    def _start_supervisor(self):
        supervisor = threading.Thread(target=self._supervisor)
        supervisor.daemon = True
        supervisor.start()

    def _serve(self):
        self._lock_ev.wait()
        self._get_task()

        global _current_task

        try:
            assert_msg = "Task '{}' not found in registered tasks".format(self.task['name'])
            assert self.task['name'] in __tasks__, assert_msg

            LOG.debug('Worker {} received task {!r}'.format(self.worker_id, self.task))

            _current_task = self.task

            self.task.func.local.worker = self.worker_id

            self._callback_launcher.fire('global.before',
                    args=(self.task, self.task['meta']), raise_exc=True)
            CallbackCenter(self.task['callbacks']).fire('task.before',
                    args=(self.task, self.task['meta']), raise_exc=True)

            result = self.task(*self.task['args'], **self.task['kwds'])

            with self._task_lock:
                self.task.set_result(result)
        except KeyboardInterrupt:
            with self._task_lock:
                LOG.debug('Task {!r} has been interrupted'.format(self.task))
                self.task.reset()
            raise
        except:
            with self._task_lock:
                self.task.set_exception(sys.exc_info())
                msg = 'Task {!r} error: {}'.format(self.task, sys.exc_info()[1])
                LOG.warn(msg, exc_info=sys.exc_info())
        finally:
            with self._task_lock:
                try:
                    CallbackCenter(self.task['callbacks']).fire('task.after',
                            args=(self.task, self.task['meta']))
                    self._callback_launcher.fire('global.after',
                            args=(self.task, self.task['meta']))
                    self._return_result()
                finally:
                    self.task = None
                    _current_task = None

    def _on_fork(self):
        import gc
        objects = set(o for o in gc.get_objects() if isinstance(o, threading._RLock))
        for o in objects:
            o.__init__()
        objects = set(o for o in gc.get_objects() if isinstance(o, socket.socket))
        for o in objects:
            try:
                o.close()
            except:
                pass

    def run(self):
        try:
            self._on_fork()
            self._callback_launcher.fire('global.fork', raise_exc=True)

            util.set_proc_name('szr worker')

            LOG.debug('Worker {} started'.format(self.worker_id))

            if sys.platform != 'win32':
                signal.signal(signal.SIGHUP, self._signal_handler)
                signal.signal(signal.SIGUSR1, self._signal_handler)

            self._task_lock = threading.Lock()

            self._start_supervisor()
            self._started_ev.set()

            self.unlock()

            while self._started_ev.is_set():
                self._serve()
        except KeyboardInterrupt:
            return
        except:
            LOG.exception('Worker {} error: {}'.format(self.worker_id, sys.exc_info()[:2]))
        finally:
            LOG.debug('Worker {} exited'.format(self.worker_id))
            self._worker_uuid = None
            self._started_ev.clear()

    def start(self):
        assert not self.is_alive(), 'Worker already running'
        self._worker_uuid = uuid.uuid4().hex
        LOG.debug('Starting worker {}'.format(self._worker_uuid))
        self._ppid = os.getpid()
        super(Worker, self).start()

    def terminate(self):
        if self.is_alive():
            LOG.debug('Terminating worker {}'.format(self.worker_id))
            for pid in util.get_children(self.pid, recursive=True):
                try:
                    os.kill(pid, SIGKILL)
                except OSError as e:
                    if e.errno != 3:  # No such process
                        raise
            os.kill(self.pid, SIGKILL)
            self.join()

    def wait_start(self, timeout=None):
        if not self._started_ev.wait(timeout=timeout):
            raise TimeoutError()


class AsyncResult(object):

    result_poll_timeout = 1
    ready_event_timeout = 10

    def __init__(self, task):
        """
        :param task: Task instance or task_id
        :type task: Task or str
        """

        if isinstance(task, Task):
            self._task = task
        elif isinstance(task, basestring):
            self._task = Task.load_by_id(task)
        else:
            raise TypeError('Argument task must be instance of Task class or task_id')

        assert_msg = "Task '{}' not found".format(self._task['task_id'])
        assert self._task.in_db(), assert_msg

        self._ready_event = Executor.ready_events.get(self._task['task_id'], None)

    def __getattr__(self, name):
        assert name in self._task.schema, "'%s' not in Task schema" % name
        if name not in ('task_id', 'name', 'args', 'kwds'):
            self._task.load()
        return self._task[name]

    def wait(self, timeout=None):
        raise_timeout_error = bool(timeout)
        wait_time = self.ready_event_timeout if self._ready_event else self.result_poll_timeout

        # loop until exec time is greater than timeout
        # or task state is changed to completed or failed
        while True:
            self._task.load()
            if self._task['state'] not in ('pending', 'running'):
                break

            if raise_timeout_error:
                if timeout <= 0:
                    raise TimeoutError()
                if wait_time > timeout:
                    wait_time = timeout
                timeout -= wait_time

            if self._ready_event:
                self._ready_event.wait(timeout=wait_time)
            else:
                time.sleep(wait_time)

    def get(self, timeout=None):
        self.wait(timeout=timeout)
        if self._task['state'] == 'failed':
            raise self._task.exception
        else:
            return self._task.result

    def revoke(self):
        Executor.revoke(self._task['task_id'])
