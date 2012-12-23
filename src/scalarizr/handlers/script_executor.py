from __future__ import with_statement
'''
Created on Dec 24, 2009

@author: marat
'''
from __future__ import with_statement

from scalarizr.bus import bus
#from scalarizr.config import STATE
from scalarizr import config as szrconfig
from scalarizr.handlers import Handler, HandlerError
from scalarizr.messaging import Queues, Messages
from scalarizr.util import parse_size, format_size, read_shebang, split_strip
from scalarizr.util.filetool import write_file
from scalarizr.config import ScalarizrState
from scalarizr.handlers import operation

import time
import ConfigParser
import subprocess
import threading
import os
import shutil
import stat
import signal
import logging
import binascii
import Queue


def get_handlers():
	return [ScriptExecutor()]


LOG = logging.getLogger(__name__)

skip_events = set()
"""
@var ScriptExecutor will doesn't request scripts on passed events
"""

exec_dir_prefix = '/usr/local/bin/scalr-scripting.'
logs_dir = '/var/log/scalarizr/scripting'
logs_truncate_over = 20 * 1000


def get_truncated_log(logfile, maxsize=None):
	maxsize = maxsize or logs_truncate_over
	f = open(logfile, "r")
	try:
		ret = unicode(f.read(int(maxsize)), 'utf-8')
		if (os.path.getsize(logfile) > maxsize):
			ret += u"... Truncated. See the full log in " + logfile.encode('utf-8')
		return ret.encode('utf-8')
	finally:
		f.close()


class ScriptExecutor(Handler):
	name = 'script_executor'

	def __init__(self):
		self.queue = Queue.Queue()
		self.in_progress = []
		bus.on(
			init=self.on_init,
			start=self.on_start,
			shutdown=self.on_shutdown
		)

		# Operations
		self._op_exec_scripts = 'Execute scripts'
		self._step_exec_tpl = "Execute '%s' in %s mode"

		# Services
		self._cnf = bus.cnf
		self._queryenv = bus.queryenv_service
		self._platform = bus.platform

	def on_init(self):
		global exec_dir_prefix, logs_dir, logs_truncate_over

		# Configuration
		cnf = bus.cnf
		ini = cnf.rawini

		# read exec_dir_prefix
		try:
			exec_dir_prefix = ini.get(self.name, 'exec_dir_prefix')
			if not os.path.isabs(exec_dir_prefix):
				os.path.join(bus.base_path, exec_dir_prefix)
		except ConfigParser.Error:
			pass

		# read logs_dir_prefix
		try:
			logs_dir = ini.get(self.name, 'logs_dir')
			if not os.path.exists(logs_dir):
				os.makedirs(logs_dir)
		except ConfigParser.Error:
			pass

		# logs_truncate_over
		try:
			logs_truncate_over = parse_size(ini.get(self.name, 'logs_truncate_over'))
		except ConfigParser.Error:
			pass

		self.log_rotate_thread = threading.Thread(name='ScriptingLogRotate',
									target=LogRotateRunnable())
		self.log_rotate_thread.setDaemon(True)

	def on_start(self):
		# Start log rotation
		self.log_rotate_thread.start()

		# Restore in-progress scripts
		LOG.debug('STATE[script_executor.in_progress]: %s', szrconfig.STATE['script_executor.in_progress'])
		scripts = [Script(**kwds) for kwds in szrconfig.STATE['script_executor.in_progress'] or []]
		LOG.debug('Restoring %d in-progress scripts', len(scripts))

		for sc in scripts:
			self._execute_one_script(sc)

	def on_shutdown(self):
		# save state
		LOG.debug('Saving Work In Progress (%d items)', len(self.in_progress))
		szrconfig.STATE['script_executor.in_progress'] = [sc.state() for sc in self.in_progress]

	def _execute_one_script(self, script):
		if script.asynchronous:
			threading.Thread(target=self._execute_one_script0,
							args=(script, )).start()
		else:
			self._execute_one_script0(script)

	def _execute_one_script0(self, script):
		try:
			self.in_progress.append(script)
			if not script.start_time:
				script.start()
			self.send_message(Messages.EXEC_SCRIPT_RESULT, script.wait(), queue=Queues.LOG)
		except:
			if script.asynchronous:
				LOG.exception('Caught exception')
			raise
		finally:
			self.in_progress.remove(script)

	def execute_scripts(self, scripts):
		if not scripts:
			return

		if scripts[0].event_name:
			phase = "Executing %d %s script(s)" % (len(scripts), scripts[0].event_name)
		else:
			phase = 'Executing %d script(s)' % (len(scripts), )
		self._logger.info(phase)

		if self._cnf.state != szrconfig.ScalarizrState.INITIALIZING:
			# Define operation
			op = operation(name=self._op_exec_scripts, phases=[{
				'name': phase,
				'steps': ["Execute '%s'" % script.name for script in scripts if not script.asynchronous]
			}])
			op.define()
		else:
			op = bus.initialization_op

		with op.phase(phase):
			for script in scripts:
				step_title = self._step_exec_tpl % (script.name, 'async' if script.asynchronous else 'sync')
				with op.step(step_title):
					self._execute_one_script(script)

	def accept(self, message, queue, behaviour=None, platform=None, os=None, dist=None):
		return not message.name in skip_events

	def __call__(self, message):
		event_name = message.event_name if message.name == Messages.EXEC_SCRIPT else message.name
		role_name = message.role_name if hasattr(message, 'role_name') else 'unknown_role'
		LOG.debug("Scalr notified me that '%s' fired", event_name)

		if self._cnf.state == ScalarizrState.IMPORTING:
			LOG.debug('Scripting is OFF when state: %s', ScalarizrState.IMPORTING)
			return

		scripts = []

		if 'scripts' in message.body:
			if not message.body['scripts']:
				self._logger.debug('Empty scripts list. Breaking')
				return

			LOG.debug('Fetching scripts from incoming message')
			scripts = [Script(name=item['name'], body=item['body'],
							asynchronous=int(item['asynchronous']),
							exec_timeout=item['timeout'], event_name=event_name,
							role_name=role_name,
							event_server_id=message.body.get('server_id'))
						for item in message.body['scripts']]
		else:
			LOG.debug("Fetching scripts for event %s", event_name)
			event_id = message.meta['event_id'] if message.name == Messages.EXEC_SCRIPT else None
			target_ip = message.body.get('local_ip')
			local_ip = self._platform.get_private_ip()

			queryenv_scripts = self._queryenv.list_scripts(event_name, event_id,
										target_ip=target_ip, local_ip=local_ip)
			scripts = [Script(name=s.name, body=s.body, asynchronous=s.asynchronous,
						exec_timeout=s.exec_timeout, event_name=event_name, role_name=role_name) \
						for s in queryenv_scripts]

		LOG.debug('Fetched %d scripts', len(scripts))
		self.execute_scripts(scripts)


class Script(object):
	name = None
	body = None
	asynchronous = None
	event_name = None
	role_name = None
	exec_timeout = 0
	event_server_id = None
	
	id = None
	pid = None
	return_code = None
	interpreter = None
	start_time = None
	exec_path = None

	logger = None
	proc = None
	stdout_path = None
	stderr_path = None

	def __init__(self, **kwds):
		'''
		Variant A:
		Script(name='AppPreStart', body='#!/usr/bin/python ...', asynchronous=True)

		Variant B:
		Script(id=43432234343, name='AppPreStart', pid=12145,
				interpreter='/usr/bin/python', start_time=4342424324, asynchronous=True)
		'''
		for key, value in kwds.items():
			setattr(self, key, value)

		assert self.name, '`name` required'
		assert self.exec_timeout, '`exec_timeout` required'

		if self.name and self.body:
			self.id = str(time.time())
			interpreter = read_shebang(script=self.body)
			if not interpreter:
				raise HandlerError("Can't execute script '%s' cause it hasn't shebang.\n"
								"First line of the script should have the form of a shebang "
								"interpreter directive is as follows:\n"
								"#!interpreter [optional-arg]" % (self.name, ))
			self.interpreter = interpreter
		else:
			assert self.id, '`id` required'
			assert self.pid, '`pid` required'
			assert self.start_time, '`start_time` required'
			if self.interpreter:
				self.interpreter = split_strip(self.interpreter)[0]

		self.logger = logging.getLogger('%s.%s' % (__name__, self.id))
		self.exec_path = os.path.join(exec_dir_prefix + self.id, self.name)
		if self.exec_timeout:
			self.exec_timeout = int(self.exec_timeout)
		args = (self.name, self.event_name, self.role_name, self.id)
		self.stdout_path = os.path.join(logs_dir, '%s.%s.%s.%s-out.log' % args)
		self.stderr_path = os.path.join(logs_dir, '%s.%s.%s.%s-err.log' % args)

	def start(self):
		# Check interpreter here, and not in __init__
		# cause scripts can create sequences when previous script
		# installs interpreter for the next one
		if not os.path.exists(self.interpreter):
			raise HandlerError("Can't execute script '%s' cause "
							"interpreter '%s' not found" % (self.name, self.interpreter))

		# Write script to disk, prepare execution
		exec_dir = os.path.dirname(self.exec_path)
		if not os.path.exists(exec_dir):
			os.makedirs(exec_dir)

		write_file(self.exec_path, self.body.encode('utf-8'), logger=LOG)
		os.chmod(self.exec_path, stat.S_IREAD | stat.S_IEXEC)

		stdout = open(self.stdout_path, 'w+')
		stderr = open(self.stderr_path, 'w+')

		# Start process
		self.logger.debug('Executing %s'
				'\n  %s'
				'\n  1>%s'
				'\n  2>%s'
				'\n  timeout: %s seconds',
				self.interpreter, self.exec_path, self.stdout_path,
				self.stderr_path, self.exec_timeout)
		self.proc = subprocess.Popen(self.exec_path, stdout=stdout,
							stderr=stderr, close_fds=True)
		self.pid = self.proc.pid
		self.start_time = time.time()

	def wait(self):
		try:
			# Communicate with process
			self.logger.debug('Communicating with %s (pid: %s)', self.interpreter, self.pid)
			while time.time() - self.start_time < self.exec_timeout:
				if self._proc_poll() is None:
					time.sleep(0.5)
				else:
					# Process terminated
					self.logger.debug('Process terminated')
					self.return_code = self._proc_complete()
					break
			else:
				# Process timeouted
				self.logger.debug('Timeouted: %s seconds. Killing process %s (pid: %s)',
									self.exec_timeout, self.interpreter, self.pid)
				self.return_code = self._proc_kill()

			elapsed_time = time.time() - self.start_time
			self.logger.debug('Finished %s'
					'\n  %s'
					'\n  1: %s'
					'\n  2: %s'
					'\n  return code: %s'
					'\n  elapsed time: %s',
					self.interpreter, self.exec_path,
					format_size(os.path.getsize(self.stdout_path)),
					format_size(os.path.getsize(self.stderr_path)),
					self.return_code,
					elapsed_time)

			ret = dict(
				stdout=binascii.b2a_base64(get_truncated_log(self.stdout_path)),
				stderr=binascii.b2a_base64(get_truncated_log(self.stderr_path)),
				time_elapsed=elapsed_time,
				script_name=self.name,
				script_path=self.exec_path,
				event_name=self.event_name or '',
				return_code=self.return_code,
				event_server_id=self.event_server_id
			)
			return ret

		except:
			if threading.currentThread().name != 'MainThread':
				self.logger.exception('Exception in script execution routine')
			else:
				raise

		finally:
			f = os.path.dirname(self.exec_path)
			if os.path.exists(f):
				shutil.rmtree(f)

	def state(self):
		return {
			'id': self.id,
			'pid': self.pid,
			'name': self.name,
			'interpreter': self.interpreter,
			'start_time': self.start_time,
			'asynchronous': self.asynchronous,
			'event_name': self.event_name,
			'role_name': self.role_name,
			'exec_timeout': self.exec_timeout
		}

	def _proc_poll(self):
		if self.proc:
			return self.proc.poll()
		else:
			statfile = '/proc/%s/stat' % self.pid
			exefile = '/proc/%s/exe' % self.pid
			if os.path.exists(exefile) and os.readlink(exefile) == self.interpreter:
				stat = open(statfile).read().strip().split(' ')
				if stat[2] not in ('Z', 'D'):
					return None

			return 0

	def _proc_kill(self):
		self.logger.debug('Timeouted: %s seconds. Killing process %s (pid: %s)',
							self.exec_timeout, self.interpreter, self.pid)
		if self.proc and hasattr(self.proc, "kill"):
			self.proc.kill()
		else:
			os.kill(self.pid, signal.SIGKILL)
		return -9

	def _proc_complete(self):
		if self.proc:
			return self.proc.returncode
		else:
			return 0


class LogRotateRunnable(object):
	def __call__(self):
		while True:
			files = os.listdir(logs_dir)
			files.sort()
			for file in files[0:-100]:
				os.remove(os.path.join(logs_dir, file))
			time.sleep(3600)
