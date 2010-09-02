'''
Created on Jan 6, 2010

@author: marat
@author: Dmytro Korsakov
'''
from scalarizr.bus import bus
from scalarizr.config import Configurator, BuiltinBehaviours, ScalarizrState
from scalarizr.handlers import Handler, HandlerError
from scalarizr.messaging import Messages
from scalarizr.util import system, cached, firstmatched,\
	validators
from scalarizr.util.filetool import read_file, write_file
import os
import re
import shutil
import subprocess
import logging
from datetime import datetime
from scalarizr.util import initdv2
from scalarizr.libs.metaconf import Configuration


BEHAVIOUR = BuiltinBehaviours.WWW
CNF_NAME = BEHAVIOUR
CNF_SECTION = BEHAVIOUR

BIN_PATH = 'binary_path'
APP_PORT = 'app_port'
HTTPS_INC_PATH = 'https_include_path'
APP_INC_PATH = 'app_include_path'

class NginxInitScript(initdv2.ParametrizedInitScript):
	def __init__(self):
		initd_script = "/etc/init.d/nginx"
		
		if not os.path.exists(initd_script):
			raise HandlerError("Cannot find Nginx init script at %s. Make sure that nginx is installed" % initd_script)

		pid_file = None
		try:
			out = system("nginx -V")[1]
			m = re.search("--pid-path=(.*?)\s", out)
			if m:
					pid_file = m.group(1)
		except:
			pass
		
		initdv2.ParametrizedInitScript.__init__(self, 'nginx', 
				initd_script, pid_file, socks=[initdv2.SockParam(80)])
		
	def status(self):
		pass
		# TODO: Connect to 80 port and make sure that server is nginx, not apache 

initdv2.explore('nginx', NginxInitScript)

# Nginx behaviours configuration options
class NginxOptions(Configurator.Container):
	'''
	www behaviour
	'''
	cnf_name = CNF_NAME
	
	class binary_path(Configurator.Option):
		'''
		Path to nginx binary
		'''
		name = CNF_SECTION + '/binary_path'
		required = True
		
		@property
		@cached
		def default(self):
			return firstmatched(lambda p: os.access(p, os.F_OK | os.X_OK), 
					('/usr/sbin/nginx',	'/usr/local/nginx/sbin/nginx'), '')

		@validators.validate(validators.executable)
		def _set_value(self, v):
			Configurator.Option._set_value(self, v)
			
		value = property(Configurator.Option._get_value, _set_value)


	class app_port(Configurator.Option):
		'''
		App role port
		'''
		name = CNF_SECTION + '/app_port'
		default = '80'
		required = True
		
		@validators.validate(validators.portnumber())
		def _set_value(self, v):
			Configurator.Option._set_value(self, v)
		
		value = property(Configurator.Option._get_value, _set_value)
		

	class app_include_path(Configurator.Option):
		'''
		App upstreams configuration file path.
		'''
		name = CNF_SECTION + '/app_include_path'
		default = '/etc/nginx/app-servers.include'
		required = True
		
	class https_include_path(Configurator.Option):
		'''
		HTTPS configuration file path.
		'''
		name = CNF_SECTION + '/https_include_path'
		default = '/etc/nginx/https.include'
		required = True


def get_handlers():
	return [NginxHandler()]

class NginxHandler(Handler):
	
	def __init__(self):
		self._logger = logging.getLogger(__name__)
		self._queryenv = bus.queryenv_service	
		self._cnf = bus.cnf
		self._initd = initdv2.lookup('nginx')
		
		ini = self._cnf.rawini
		self._https_conf_path = ini.get(CNF_SECTION, HTTPS_INC_PATH)
		self._nginx_binary = ini.get(CNF_SECTION, BIN_PATH)
		self._app_port = ini.get(CNF_SECTION, APP_PORT)
		self._include = ini.get(CNF_SECTION, APP_INC_PATH)
		bus.define_events("nginx_upstream_reload")
		bus.on("init", self.on_init)

		
	def on_init(self):
		bus.on("start", self.on_start)
		bus.on('before_host_up', self.on_before_host_up)
		bus.on("before_host_down", self.on_before_host_down)
		
	def on_start(self, *args):
		if self._cnf.state == ScalarizrState.RUNNING:
			try:
				self._logger.info("Starting Nginx")
				self._initd.start()
			except initdv2.InitdError, e:
				self._logger.error(e)	
				
	on_before_host_up = on_start
	
	def on_HostUp(self, message):
		self.nginx_upstream_reload()
	
	def on_HostDown(self, message):
		self.nginx_upstream_reload()
	
	def nginx_upstream_reload(self, force_reload=False):

		config_dir = os.path.dirname(self._include)
		nginx_conf_path = config_dir + '/nginx.conf'
		
		if not hasattr(self, '_config'):
			try:
				self._config = Configuration('nginx')
				self._config.read(nginx_conf_path)
			except (Exception, BaseException), e:
				raise HandlerError('Cannot read/parse nginx main configuration file: %s' % str(e))

		template_path = os.path.join(self._cnf.public_path(), "nginx/app-servers.tpl")
		
		backend_include = Configuration('nginx')
		if not os.path.exists(template_path):
			'''
			template_content = """\nupstream backend {\n\tip_hash;\n\n\t${upstream_hosts}\n}\n"""
			log_message = "nginx template '%s' doesn't exists. Creating default template" % (template_path,)
			write_file(template_path, template_content, msg = log_message, logger = self._logger)
			'''
			backend_include.add('upstream', 'backend')
			backend_include.add('upstream/ip_hash')
			backend_include.write(open(template_path, 'w'))
		else:
			backend_include.read(template_path)

		# Create upstream hosts configuration
		for app_serv in self._queryenv.list_roles(behaviour = BuiltinBehaviours.APP):
			for app_host in app_serv.hosts :
				server_str = '%s:%s' % (app_host.internal_ip, self._app_port)
				backend_include.add('upstream/server', server_str)
		if not backend_include.get_list('upstream/server'):
			self._logger.debug("Scalr returned empty app hosts list. Adding localhost only.")
			backend_include.add('upstream/server', '127.0.0.1:80')
		
		#HTTPS Configuration
		# openssl req -new -x509 -days 9999 -nodes -out cert.pem -keyout cert.key
		cert_path = self._cnf.key_path("https.crt")
		pk_path = self._cnf.key_path("https.key") 
		if os.path.isfile(bus.etc_path+"/nginx/https.include") and 	os.path.isfile(cert_path) \
															   and  os.path.isfile(pk_path):
			https_include = bus.etc_path + "/nginx/https.include;"
			self._logger.debug("Adding %s to template", https_include)
			backend_include.add('include', https_include)
			#Determine, whether configuration was changed or not
		
		old_include = None
		if os.path.isfile(self._include):
			self._logger.debug("Reading old configuration from %s" % self._include)
			old_include = Configuration('nginx')
			old_include.read(self._include)
			
		if old_include and not force_reload and \
							backend_include.get_list('upstream/server') == old_include.get_list('upstream/server') :
			self._logger.debug("nginx upstream configuration wasn`t changed.")
		else:
			self._logger.debug("nginx upstream configuration was changed.")
			self._logger.debug("Creating backup config files.")
			if os.path.isfile(self._include):
				shutil.move(self._include, self._include+".save")
			else:
				self._logger.debug('%s does not exist. Nothing to backup.' % self._include)
				
			self._logger.debug("Writing template to %s" % self._include)
			backend_include.write(open(self._include, 'w'))
			#Patching main config file
			if 'http://backend' in self._config.get_list('http/server/location/proxy_pass') and \
						self._include in self._config.get_list('http/include'):
				
				self._logger.debug("File %s already included into nginx main config %s", 
								self._include, nginx_conf_path)
			else:
				self._config.comment('http/server')
				self._config.read(os.path.join(self._cnf.private_path(), "nginx/server.tpl"))
				self._config.add('http/include', self._include)
				self._config.write(open(nginx_conf_path, 'w'))
			
				self._logger.info("Testing new configuration.")
			
				if os.path.isfile(self._nginx_binary):
				
					nginx_test_command = [self._nginx_binary, "-t"]
				
					p = subprocess.Popen(nginx_test_command, 
							stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
					stdout, stderr = p.communicate()
					is_nginx_test_failed = p.poll()
					
					if is_nginx_test_failed:
						self._logger.error("Configuration error detected:" +  stderr + " Reverting configuration.")
						if os.path.isfile(self._include):
							shutil.move(self._include, self._include+".junk")
						else:
							self._logger.debug('%s does not exist', self._include)
						if os.path.isfile(self._include+".save"):
							shutil.move(self._include+".save", self._include)
						else:
							self._logger.debug('%s does not exist', self._include+".save")
					else:
						# Reload nginx
						self._initd.reload()

		bus.fire("nginx_upstream_reload")
	
	
	def on_before_host_down(self, *args):
		try:
			self._logger.info("Stopping Nginx")
			self._initd.stop()
		except initdv2.InitdError, e:
			self._logger.error("Cannot stop nginx: %s" % str(e))
			if self._initd.running:
				raise

		
	def on_BeforeHostTerminate(self, message):
		config = bus.config
		include_path = config.get(CNF_SECTION, "app_include_path")
		if not os.path.exists(include_path):
			return

		include = Configuration('nginx')	
		include.read(include_path)
		server_ip = '%s:80' % message.local_ip or message.remote_ip
		if server_ip in include.get_list('upstream/server'):
			include.remove('upstream/server', server_ip)
		include.write(open(include_path, 'w'))
		self._initd.restart()

	def _update_vhosts(self):
		self._logger.debug("Requesting virtual hosts list")
		received_vhosts = self._queryenv.list_virtual_hosts()
		self._logger.debug("Virtual hosts list obtained (num: %d)", len(received_vhosts))
		
		https_config = ''
		
		if [] != received_vhosts:
			
			https_certificate = self._queryenv.get_https_certificate()
			
			cert_path = self._cnf("https.crt")
			pk_path = self._cnf("https.key")
			
			if https_certificate[0]:
				msg = 'Writing ssl cert' 
				cert = https_certificate[0]
				write_file(cert_path, cert, msg=msg, logger=self._logger)
			else:
				self._logger.error('Scalr returned empty SSL Cert')
				return
				
			if len(https_certificate)>1 and https_certificate[1]:
				msg = 'Writing ssl key'
				pk = https_certificate[1]
				write_file(pk_path, pk, msg=msg, logger=self._logger)
			else:
				self._logger.error('Scalr returned empty SSL Cert')
				return

			for vhost in received_vhosts:
				if vhost.hostname and vhost.type == 'nginx': #and vhost.https
					raw = vhost.raw.replace('/etc/aws/keys/ssl/https.crt',cert_path)
					raw = raw.replace('/etc/aws/keys/ssl/https.key',pk_path)
					https_config += raw + '\n'

		else:
			self._logger.debug('Scalr returned empty virtualhost list')

		if https_config:

			if os.path.exists(self._https_conf_path) and read_file(self._https_conf_path, logger=self._logger):
				time_suffix = str(datetime.now()).replace(' ','.')
				shutil.move(self._https_conf_path, self._https_conf_path + time_suffix)

			msg = 'Writing virtualhosts to https.include'
			write_file(self._https_conf_path, https_config, msg=msg, logger=self._logger)
		

	def on_VhostReconfigure(self, message):
		self._logger.info("Received virtual hosts update notification. Reloading virtual hosts configuration")
		self._update_vhosts()
		self.nginx_upstream_reload(True)
	
	
	def accept(self, message, queue, behaviour=None, platform=None, os=None, dist=None):
		return BEHAVIOUR in behaviour and \
			(message.name == Messages.HOST_UP or \
			message.name == Messages.HOST_DOWN or \
			message.name == Messages.BEFORE_HOST_TERMINATE or \
			message.name == Messages.VHOST_RECONFIGURE)	