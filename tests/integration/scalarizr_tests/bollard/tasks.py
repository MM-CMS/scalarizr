from scalarizr import bollard

import time
import threading


@bollard.task()
def foo():
    return


@bollard.task(name='not bar')
def bar():
    return


@bollard.task()
def foo_args_kwds(*args, **kwds):
    assert args == (1, 2), args
    assert kwds == {'1': 1, '2': 2}, kwds
    return True


@bollard.task()
def foo_int_result():
    return 0


@bollard.task()
def foo_str_result():
    return '0'


@bollard.task()
def foo_exc_result():
    1 / 0


@bollard.task()
def soft_lock():
    while True:
        time.sleep(0.01)


@bollard.task()
def hard_lock():
    threading.Event().wait()


@bollard.task()
def sleep(seconds):
    time.sleep(seconds)


@bollard.task(name='bind', bind=True)
def bind(self, *args, **kwds):
    assert isinstance(self, bollard.Task)


@bollard.task(name='current task')
def current_task(*args, **kwds):
    assert isinstance(bollard.current_task(), bollard.Task), bollard.current_task()
    return True
