'''
Created on Aug 1, 2012

@author: dmitry
'''

'''
Created on Nov 25, 2011

@author: marat
'''


from __future__ import with_statement

import time
import logging
import threading
from scalarizr.bus import bus
from scalarizr import handlers, rpc
from scalarizr.services import redis as redis_service
from scalarizr.handlers import redis as redis_handler

BEHAVIOUR = CNF_SECTION = redis_handler.CNF_SECTION
OPT_REPLICATION_MASTER = redis_handler.OPT_REPLICATION_MASTER
OPT_PERSISTENCE_TYPE = redis_handler.OPT_PERSISTENCE_TYPE
STORAGE_PATH = redis_handler.STORAGE_PATH


LOG = logging.getLogger(__name__)


class RedisAPI(object):

	_cnf = None
	_queryenv = None
	
	def __init__(self):
		self._cnf = bus.cnf
		self._queryenv = bus.queryenv_service


	@property
	def is_replication_master(self):
		value = 0
		if self._cnf.rawini.has_section(CNF_SECTION) and self._cnf.rawini.has_option(CNF_SECTION, OPT_REPLICATION_MASTER):
			value = self._cnf.rawini.get(CNF_SECTION, OPT_REPLICATION_MASTER)
		return True if int(value) else False
	
	
	@property
	def persistence_type(self):
		value = 'snapshotting'
		if self._cnf.rawini.has_section(CNF_SECTION) and self._cnf.rawini.has_option(CNF_SECTION, OPT_PERSISTENCE_TYPE):
			value = self._cnf.rawini.get(CNF_SECTION, OPT_PERSISTENCE_TYPE)
		return value


	def get_primary_ip(self):
		master_host = None
		LOG.info("Requesting master server")
		while not master_host:
			try:
				master_host = list(host 
					for host in self._queryenv.list_roles(self._role_name)[0].hosts 
					if host.replication_master)[0]
			except IndexError:
				LOG.debug("QueryEnv respond with no %s master. " % BEHAVIOUR + 
						"Waiting %d seconds before the next attempt" % 5)
				time.sleep(5)
		host = master_host.internal_ip or master_host.external_ip
		return host


	def _start_processes(self, ports=[], passwords=[]):
		redis_instances = redis_service.RedisInstances(self.is_replication_master, self.persistence_type)
		redis_instances.init_processes(ports, passwords)
		if self.is_replication_master:
			res = redis_instances.init_as_masters(mpoint=STORAGE_PATH)
		else:
			primary_ip = self.get_primary_ip()
			assert primary_ip is not None
			res = redis_instances.init_as_slaves(mpoint=STORAGE_PATH, primary_ip=primary_ip)
		return res
	
	
	@rpc.service_method
	def launch_processes(self, num=None, ports=None, passwords=None, async=False):	
		if ports and passwords and len(ports) != len(passwords):
			raise AssertionError('Number of ports must be equal to number of passwords')
		if num and ports and num != len(ports):
				raise AssertionError('When ports range is passed its length must be equal to num parameter')
		if not self.is_replication_master:
			if not passwords or not ports:
				raise AssertionError('ports and passwords are required to launch processes on redis slave')
		
		if async:
			txt = 'Launch Redis processes'
			op = handlers.operation(name=txt)
			def block():
				op.define()
				with op.phase(txt):
					with op.step(txt):
						result = self._start_processes(ports, passwords)
				op.ok(data=dict(ports=result[0], passwords=result[1]))
			threading.Thread(target=block).start()
			return op.id
		else:
			result = self._start_processes(ports, passwords)
			return dict(ports=result[0], passwords=result[1])

		
	@rpc.service_method
	def shutdown_processes(self, ports, remove_data=False, async=False):
		redis_instances = redis_service.RedisInstances()
		redis_instances.init_processes(ports)
		if async:
			txt = 'Shutdown Redis processes'
			op = handlers.operation(name=txt)
			def block():
				op.define()
				with op.phase(txt):
					with op.step(txt):
						redis_instances.kill_processes(ports, remove_data)
				op.ok()
			threading.Thread(target=block).start()
			return op.id
		else:
			redis_instances.kill_processes(ports, remove_data)
		