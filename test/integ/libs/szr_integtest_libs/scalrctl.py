'''
Created on Sep 23, 2010

@author: marat
'''

from szr_integtest import config
from szr_integtest_libs.ssh_tool import execute, SshManager

import logging
import time
import os
from scalarizr.util import wait_until
import urllib
import httplib2
import copy

try:
	import json
except:
	import simplejson as json
log_path = os.path.expanduser('~/.scalr-dev/logs')
server_info_url = 'http://scalr-dev.local.webta.net/servers/extendedInfo'

class FarmUIError(Exception):
	pass

EC2_ROLE_DEFAULT_SETTINGS = {
	'aws.availability_zone' : 'us-east-1a',
	'aws.instance_type' : 't1.micro',
}

EC2_MYSQL_ROLE_DEFAULT_SETTINGS = {
	'mysql.ebs_volume_size' : '1'
}

class ScalrConsts:
	class Platforms:
		PLATFORM_EC2 	= 'Amazon EC2'
		PLATFORM_RACKSPACE 	= 'Rackspace'
	class Behaviours:
		BEHAVIOUR_BASE  = 'Base'
		BEHAVIOUR_APP   = 'Apache'
		BEHAVIOUR_MYSQL = 'MySQL' 
		BEHAVIOUR_WWW = 'Nginx'
		BEHAVIOUR_MEMCACHED = 'Memcached'
		
platforms = {'ec2':'Amazon EC2', 
			'rackspace':'Rackspace'}	

class FarmUI:
	sel = None
	farm_id = None	
	servers = None
	
	def _login(f):
		def g(self, *args, **kwargs):
			if not hasattr(self.sel, '_logged_in') or not self.sel._logged_in:
				ui_login(self.sel)
			return f(self, *args, **kwargs)
		return g	

	def _open_farm_builder(f):				
		def g(self, *args, **kwargs):
			if not 'farms_builder.php?id=' in self.sel.get_location():
				self.use(self.farm_id)
			return f(self, *args, **kwargs)
		return g	
	
	def __init__(self, sel):
		self.sel = sel
		self.servers = []
		self.farm_id = config.get('test-farm/farm_id')
	
	def use(self, farm_id=None):
		if 'farms_builder.php?id=%s' % self.farm_id in self.sel.get_location():
			return
		self.servers = []
		ui_login(self.sel)
		self.sel.open('farms_builder.php?id=%s' % self.farm_id)
		wait_until(lambda: self.sel.is_element_present('//span[text()="Roles"]'), sleep=0.1, timeout=10)
	
	@_login
	@_open_farm_builder
	def add_role(self, role_name, min_servers=1, max_servers=2, settings=None):
			
		role_id = self.get_role_id(role_name)
		
		settings = settings or dict()
		if not 'aws.instance_type' in settings:
			settings['aws.instance_type'] = 't1.micro'
		settings['scaling.min_instances'] = settings.get('scaling.min_instances', min_servers)
		settings['scaling.max_instances'] = settings.get('scaling.max_instances', max_servers)

		wait_until(lambda: self.sel.is_element_present('//span[text()="Roles"]'), sleep=0.1, timeout=10)
		self.sel.click('//span[text()="Roles"]')
		self.sel.click('//div[@class="viewers-selrolesviewer-blocks viewers-selrolesviewer-add"]')
		self.sel.wait_for_condition("selenium.browserbot.getCurrentWindow().document.getElementById('viewers-addrolesviewer')", 10000)
		try:
			self.sel.click('//li[@itemid="%s"]' % role_id)
			time.sleep(0.5)
			self.sel.click('//li[@itemid="%s"]/div[@class="info"]/img[1]' % role_id)
			if self.sel.is_element_present('//label[text()="Location:"]'):
				self.sel.click('//label[text()="Platform:"]')
				self.sel.click('//div[text()="%s"]' % platforms[self.platform])
				self.sel.click('//label[text()="Location:"]')
				self.sel.click('//div[@class="x-combo-list-inner"]/div[text()="AWS / US East 1"]')
				self.sel.click('//button[text()="Add"]')
		except:
			raise Exception("Role '%s' doesn't exist" % role_name)
		
		
		self.edit_role(role_name, settings)
	
	@_login
	@_open_farm_builder
	def _role_in_farm(self, role_name):
		
		wait_until(lambda: self.sel.is_element_present('//span[text()="Roles"]'), sleep=0.1, timeout=10)
		self.sel.click('//span[text()="Roles"]')
		
		try:
			try:
				self.sel.click('//span[@class="short" and text()="%s"]' % role_name)
			except:				
				self.sel.click('//div[@class="full" and text()="%s"]' % role_name)
		except:
			return False
		return True
	
	@_login
	@_open_farm_builder
	def edit_role(self, role_name, settings=None):
		
		role_opts = copy.copy(settings)
		
		wait_until(lambda: self.sel.is_element_present('//span[text()="Roles"]'), sleep=0.1, timeout=10)
		self.sel.click('//span[text()="Roles"]')

		if not self._role_in_farm(role_name):
			raise Exception("Farm '%s' doesn't have role '%s'" % (self.farm_id, role_name))
		
		i = 1
		while role_opts:
			try:
				self.sel.click('//div[@class="viewers-farmrolesedit-tabs"]/div[not(@style)][%s]' % i)
				time.sleep(0.5)
				wait_until(lambda: not self.sel.is_element_present('//html/body/div[@class="ext-el-mask-msg x-mask-loading"]/div'), timeout=10, sleep=0.5)
				time.sleep(0.5)
				for option, value in settings.iteritems():
					el_xpath = '//input[@name = "%s"]' % option
					if self.sel.is_element_present(el_xpath) and self.sel.is_visible(el_xpath):
						try:
							id = self.sel.get_attribute('//div[@class=" x-panel x-panel-noborder"]//*[@name="%s"]/@id' % option)
							self.sel.run_script("with (Ext.getCmp('%s')) { setValue('%s'); fireEvent('select'); }" % (id, value))
							del(role_opts[option])
						except:
							pass
				time.sleep(0.5)
				i += 1
			except (Exception, BaseException), e:
				break
				
		self.sel.click('//div[@class="viewers-selrolesviewer-blocks viewers-selrolesviewer-add"]')

	@_login
	@_open_farm_builder
	def remove_role(self, role_name):
			
		self.sel.click('//span[text()="Roles"]')
		
		try:
			self.sel.click('//div[text()="%s"]/../a' % role_name)
			self.sel.click('//button[text()="Yes"]')
		except:
			raise Exception("Role '%s' doesn't exist" % role_name)

	@_login
	@_open_farm_builder	
	def remove_all_roles(self):
		self.sel.click('//span[text()="Roles"]')
		while True:
			try:
				self.sel.click('//div[@id="viewers-selrolesviewer"]/ul/li/a/')
				self.sel.click('//button[text()="Yes"]')
			except:
				break
	@_login
	@_open_farm_builder
	def save(self):
	
		wait_until(lambda: self.sel.is_element_present('//button[text() = "Save"]'), sleep=0.1, timeout=20)
		self.sel.click('//button[text() = "Save"]')
		wait_until(lambda: not self.sel.is_element_present('//div[text() = "Please wait while saving..."]'), sleep=0.2, timeout=20)
		
		while True:
			try:
				text = self.sel.get_text('//div[@id="top-messages"]/div[last()]')
				break
			except:
				continue

		if text != 'Farm successfully saved':
			raise FarmUIError('Something wrong with saving farm %s : %s' % (self.farm_id, text))


	@property		
	@_login
	def state(self):
		server_info_url = urllib.basejoin(self.sel.browserURL, 'farms/xListViewFarms/')	
		http = httplib2.Http()
		body = urllib.urlencode({'id' : self.farm_id, 'limit' : '10'})
		headers = {'Content-type': 'application/x-www-form-urlencoded',
                        'Cookie' : self.sel.get_cookie()}
		
		content = http.request(server_info_url, 'POST', body=body, headers=headers)
		data = json.loads(content[1])
		return data['data'][0]['status_txt'].lower()
	
		"""
		self.sel.open('#/farms/view?id=%s' % self.farm_id)
		#self.sel.wait_for_page_to_load(30000)
		#wait_until(lambda: self.sel.is_element_present('//html/body/div[@class="ext-el-mask-msg x-mask-loading"]/div'), timeout=10, sleep=0.5)
		wait_until(lambda: self.sel.is_element_present('//dt[@dataindex="status"]/em/span'), timeout=10, sleep=0.5)
		time.sleep(0.5)
		return self.sel.get_text('//dt[@dataindex="status"]/em/span').lower()
		"""
	@_login
	def launch(self):
	
		self.sel.open('/farms_control.php?farmid=%s' % self.farm_id)
		self.sel.wait_for_page_to_load(30000)
		#self._wait_for_page_to_load()

		if self.sel.is_text_present("Would you like to launch"):
			self.sel.click('cbtn_2')
			self.sel.wait_for_page_to_load(30000)
		else:
			self.sel.open('/')
			raise Exception('Farm %s has been already launched' % self.farm_id)
	
	@_login
	def terminate(self, remove_from_dns=True):

		self.sel.open('/farms_control.php?farmid=%s' % self.farm_id)
		if self.sel.is_text_present("You haven't saved your servers"):
			self.sel.click('cbtn_3')
			self.sel.wait_for_page_to_load(30000)
		if self.sel.is_text_present('Delete DNS zone from nameservers'):
			if remove_from_dns:
				self.sel.check('deleteDNS')
			else:
				self.sel.uncheck('deleteDNS')
			self.sel.click('cbtn_2')
			self.sel.wait_for_page_to_load(30000)
			try:
				self.sel.get_text('//div[@class="viewers-messages viewers-successmessage"]/')
			except:
				try:
					text = self.sel.get_text('//div[@class="viewers-messages viewers-errormessage"]/')
					raise FarmUIError('Something wrong with terminating farm %s : %s' % (self.farm_id, text))
				except FarmUIError, e:
					print str(e)
				except Exception, e:
					print 'Cannot terminate farm for unknown reason'
		else:
			self.sel.open('/')
			raise Exception('Farm %s has been already terminated' % self.farm_id)
	
	def get_public_ip(self, server_id, timeout = 180):
		return self._get_server_info(server_id, ('Public IP',), timeout)
	
	def get_private_ip(self, server_id, timeout = 180):
		return self._get_server_info(server_id, ('Private IP',), timeout)
	
	def get_instance_id(self, server_id, timeout = 120):
		return self._get_server_info(server_id, ('Instance ID', 'Server ID'), timeout)
	
	def get_rs_password(self, server_id, timeout = 120):
		return self._get_server_info(server_id, ('rs.admin-pass',), timeout)

	@_login
	def _get_server_info(self, server_id, field_labels, timeout):

		try:
			http = httplib2.Http()
			headers = {'Content-type': 'application/x-www-form-urlencoded',
					   'Cookie' : self.sel.get_cookie()}
			body = urllib.urlencode({'id' : server_id})
			start_time = time.time()
			while time.time() - start_time <= timeout:
				content = http.request(server_info_url, 'POST', body=body, headers=headers)[1]
				content = json.loads(content)
				if not content['success']:
					continue
				for block in content['moduleParams']:
					for param_set in block['items']:
						if not 'fieldLabel' in param_set:
							continue
						if not any(map(lambda x: x in param_set['fieldLabel'], field_labels)):
						#if not field_label in param_set['fieldLabel']:
							continue
						if param_set['value']:
							return param_set['value']
						
			else:
				raise Exception('Timeout after %s sec.' % timeout)
		except (Exception, BaseException), e:
			raise FarmUIError("Can't get %s from scalr. %s" % (field_labels[0].lower(), e))

	def create_mysql_backup(self):
		self._open_mysql_status_page()
		self.sel.click('//input[@name="run_bcp"]')

	def create_pma_users(self):
		self._open_mysql_status_page()
		try:
			self.sel.click('//input[@name="pma_request_credentials"]')
		except:
			raise FarmUIError('PhpMyAdmin user creation request has been already sent.')
		
	def create_databundle(self):
		self._open_mysql_status_page()
		try:
			self.sel.click('//input[@name="run_bundle"]')
		except:
			raise FarmUIError("Can't send databundle request")
		
	@_login	
	def _open_mysql_status_page(self):
		
		self.sel.open('/farm_mysql_info.php?farmid=%s' % self.farm_id)
		self.sel.wait_for_page_to_load(30000)
		if not self.sel.is_text_present('Replication status'):
			raise FarmUIError("Error while opening MySQL status page for farm ID=%s. Make sure your farm has MySQL role enabled." % self.farm_id)
		
	@_login
	def get_server_list(self, role_name):
		ret = []
		farm_role_id = self.get_farm_role_id(role_name)
		url = urllib.basejoin(self.sel.browserURL, 'servers/xListViewServers/')
		http = httplib2.Http()
		body = urllib.urlencode({'farmId' : self.farm_id, 'farmRoleId' : farm_role_id, 'start' : '0', 'limit' : '15'})
		headers = {'Content-type': 'application/x-www-form-urlencoded',
                        'Cookie' : self.sel.get_cookie()}
		
		content = http.request(url, 'POST', body=body, headers=headers)
		data = json.loads(content[1])
		
		for server in data['data']:
			if not 'Running' == server['status']:
				continue
			ip = server['remote_ip']
			ret.insert(0, ip) if '1' == server['ismaster'] else ret.append(ip)
		
		return ret

		# TODO: Handle situation when there is no master in role
	@_login	
	def get_role_name(self, scalr_srv_id):
		self.use(self.farm_id)
		self.sel.open('#/servers/view')  
		#self._wait_for_page_to_load()
		wait_until(lambda: self.sel.is_element_present('//div[@class="x-list-body-inner"]'), sleep=1)
		time.sleep(0.5)
		try:
			return self.sel.get_text('//a[contains(@href, "%s")]/../../../dt[@dataindex="farm_id"]/em/a[2]' % scalr_srv_id)
		except:
			raise Exception("Server with id '%s' doesn't exist." % scalr_srv_id)
		
	@_login
	def _get_role_setting(self, role_name, setting):
		server_info_url = urllib.basejoin(self.sel.browserURL, '/roles/xListViewRoles/')
	
		http = httplib2.Http()

		body = urllib.urlencode({'query' : role_name, 'limit' : '10'})
		headers = {'Content-type': 'application/x-www-form-urlencoded',
                        'Cookie' : self.sel.get_cookie()}
		
		content = http.request(server_info_url, 'POST', body=body, headers=headers)
		data = json.loads(content[1])
		
		for role in data['data']:
			if role['platforms'] == platforms[self.platform]:
				return role[setting]
		else:
			raise Exception('Cannot determine role_id of %s' % role_name)
		
	def get_role_id(self, role_name):
		return self._get_role_setting(role_name, 'id')
	
	def get_role_behaviour(self, role_name):
		return self._get_role_setting(role_name, 'behaviors')
			
	@_login	
	def get_farm_role_id(self, role_name):
		server_info_url = urllib.basejoin(self.sel.browserURL, 'server/grids/farm_roles_list.php?a=1&farmid=%s' % self.farm_id)
		http = httplib2.Http()

		headers = {'Content-type': 'application/x-www-form-urlencoded',
                        'Cookie' : self.sel.get_cookie()}
		
		content = http.request(server_info_url, 'POST', body={}, headers=headers)
		data = json.loads(content[1])
		
		for farm_role in data['data']:
			if farm_role['platform'] == self.platform and farm_role['name'] == role_name:
				return farm_role['id']
		else:
			raise Exception('Cannot determine farm role id of %s' % role_name)		
		
	def _wait_for_page_to_load(self):
		path = '//span[text()="Please wait ..."]'
		wait_until(lambda: self.sel.is_element_present(path) and not self.sel.is_visible(path), sleep=0.5)
		
	@_login	
	def configure_vhost(self, domain, role_name):
		self.remove_vhost(domain)
		
		role_id = self.get_farm_role_id(role_name)
		document_root = os.path.join('/var/www/', domain)
		self.sel.open('/apache_vhost_add.php')
		self.sel.type('domain_name', domain)
		self.sel.type('farm_target', self.farm_id)
		self.sel.type('role_target', role_id)
		self.sel.uncheck('isSslEnabled')
		self.sel.type('document_root_dir', document_root)
		self.sel.type('server_admin', 'admin@%s' % domain)		
		self.sel.click('button_js')	
	
	@_login
	def remove_vhost(self, domain):
		vhosts_list_url = urllib.basejoin(self.sel.browserURL, "server/grids/apache_vhosts_list.php")
		http = httplib2.Http()
		headers = {'Content-type': 'application/x-www-form-urlencoded',
                        'Cookie' : self.sel.get_cookie()}
		content = http.request(vhosts_list_url, 'POST', body={}, headers=headers)
		data = json.loads(content[1])
		
		for vhost in data['data']:
			if domain == vhost['domain_name']:
				break
		else:
			return				
		
		self.sel.open('apache_vhosts_view.php')
		wait_until(lambda: self.sel.is_element_present('//dl[contains(@class, "viewers-listview-row")]'), sleep=0.1, timeout=30)

		self.sel.mouse_over('//em[text()="%s"]/../../dt[last()]/em/input' % domain)
		self.sel.click('//em[text()="%s"]/../../dt[last()]/em/input' % domain)
		self.sel.click('//button[text()="With selected"]')
		self.sel.click('//span[text()="Delete"]')
		time.sleep(0.5)
		self.sel.click('//button[text()="Yes"]')	

	@_login	
	def configure_vhost_ssl(self, domain, role_name):
		self.remove_vhost(domain)
		
		document_root = os.path.join('/var/www/', domain)
		ssl_cert = '~/.scalr-dev/apache/https.crt'
		ssl_key = '~/.scalr-dev/apache/https.key'
		ca_cert = '~/.scalr-dev/apache/https-ca.crt'
		
		role_id = self.get_farm_role_id(role_name)

		self.sel.open('/apache_vhost_add.php')
		self.sel.type('domain_name', domain)
		self.sel.type('farm_target', self.farm_id)
		self.sel.type('role_target', role_id)
		self.sel.mouse_over('isSslEnabled')
		self.sel.click('isSslEnabled')
		
		self.sel.type('ssl_cert', ssl_cert)
		self.sel.type('ssl_key', ssl_key)
		self.sel.type('ca_cert', ca_cert)
		
		self.sel.type('document_root_dir', document_root)
		self.sel.type('server_admin', 'admin@%s' % domain)	
		self.sel.click('button_js')
	
	@_login		
	def get_bundle_status(self, server_id):
		#bundle_url = "http://scalr-dev.local.webta.net/server/grids/bundle_tasks.php"
		bundle_url = "http://scalr-dev.local.webta.net/bundletasks/xListViewTasks/"
		headers = {'Content-type': 'application/x-www-form-urlencoded',
                        'Cookie' : self.sel.get_cookie()}
		body = urllib.urlencode({'query' : server_id, 'limit' : '24'})
		http = httplib2.Http()
		content = http.request(bundle_url, 'POST', body=body, headers=headers)
		result = json.loads(content[1])
		return result['data'][0]['status']
		
		
	@_login
	def run_bundle(self, server_id):
		self.use()
		self.sel.open('#/servers/%s/createSnapshot' % server_id)
		wait_until(lambda: self.sel.is_element_present('//span[text()="Create new role"]'), sleep=0.1, timeout=10)
		self.sel.check('//input[@name="replaceType" and @value="replace_farm"]')
		role_name = self.sel.get_text('//label[text()="Role name:"]/../div[1]/div')
		role_name += time.strftime('-%m-%d-%H-%M')
		self.sel.type('//input[@name="roleName"]', role_name)
		self.sel.click('//button[text()="Create role"]')
		return role_name
		
	@property
	def platform(self):
		if not hasattr(self, '_platform'):
			try:
				self._platform  = os.environ['PLATFORM']
			except:
				self._platform = 'ec2'
		return self._platform
	
	def get_server_id(self, public_ip):
		url = urllib.basejoin(self.sel.browserURL, 'servers/xListViewServers/')
		http = httplib2.Http()
		body = urllib.urlencode({'farmId' : self.farm_id, 'start' : '0', 'limit' : '15'})
		headers = {'Content-type': 'application/x-www-form-urlencoded',
                        'Cookie' : self.sel.get_cookie()}
		
		content = http.request(url, 'POST', body=body, headers=headers)
		data = json.loads(content[1])
		
		for server in data['data']:
			if public_ip == server["remote_ip"]:
				return server["server_id"]
				break
		else:
			raise FarmUIError("Can't find server with IP='%s' in %s farm " % (public_ip, self.farm_id))
		
def ui_import_server(sel, platform_name, behaviour, host, role_name):
	'''
	@return: import shell command
	'''
	ui_login(sel)
	sel.open('szr_server_import.php')
	
	platforms = sel.get_select_options('//td[@class="Inner_Gray"]/table/tbody/tr[2]/td[2]/select')
	if not platform_name in platforms:
		raise Exception('Unknown platform: %s' % platform_name)
	sel.select('//td[@class="Inner_Gray"]/table/tbody/tr[2]/td[2]/select', platform_name)
	
	behaviours = sel.get_select_options('//td[@class="Inner_Gray"]/table/tbody/tr[3]/td[2]/select')
	if not behaviour in behaviours:
		raise Exception('Unknown behaviour: %s' % behaviour)
	sel.select('//td[@class="Inner_Gray"]/table/tbody/tr[3]/td[2]/select', behaviour)
	
	sel.type('//td[@class="Inner_Gray"]/table/tbody/tr[4]/td[2]/input', host)
	sel.type('//td[@class="Inner_Gray"]/table/tbody/tr[5]/td[2]/input', role_name)
	sel.click('cbtn_2')
	sel.wait_for_page_to_load(15000)
	if not sel.is_text_present('Step 2'):
		try:
			text = sel.get_text('//div[@class="viewers-messages viewers-errormessage"]/span')			
			raise FarmUIError('Something wrong with importing server: %s' % text)
		except FarmUIError, e:
			raise
		except:
			raise Exception("Can't import server for unknow reason (Step 1)")
		
	return sel.get_text('//td[@class="Inner_Gray"]/table/tbody/tr[3]/td[1]/textarea')
	
def ui_login(sel):
	
	if hasattr(sel, '_logged_in') and sel._logged_in:
		return
	try:
		login = config.get('./scalr/admin_login')
		password = config.get('./scalr/admin_password')
	except:
		raise Exception("User's ini file doesn't contain username or password")
	
	sel.delete_all_visible_cookies()
	sel.open('/')
	sel.click('//div[@class="login-trigger-header"]/a')
	wait_until(lambda: sel.is_element_present('//div[@id="login-panel"]'), sleep=0.1, timeout=15)
	sel.type('login', login)
	sel.type('pass', password)
	sel.check('keep_session')
	sel.click('//form/button')
	sel.wait_for_page_to_load(30000)
	#if sel.get_location().find('/client_dashboard.php') == -1:
	if not sel.is_element_present('//div[@id="navmenu"]'):
		raise Exception('Login failed.')
	sel._logged_in = True

def reset_farm(ssh, farm_id):
	pass

class ScalrCtl:
	def __init__(self, farmid=None):
		self._logger = logging.getLogger(__name__)
		
		self.farmid = farmid
		scalr_host = config.get('./scalr/hostname')
		ssh_key_path = config.get('./scalr/ssh_key_path')
		
		if not os.path.exists(ssh_key_path):
			raise Exception("Key file %s doesn't exist" % ssh_key_path)
		ssh_key_password = config.get('./scalr/ssh_key_password')
		self.ssh = SshManager(scalr_host, ssh_key_path, key_pass = ssh_key_password)
		self.ssh.connect()
		
		self.channel = self.ssh.get_root_ssh_channel()
		self._logger.info('Estabilished connection to %s' % scalr_host)
		if not os.path.isdir(log_path):
			os.makedirs(log_path)		

	def exec_cronjob(self, name, server_id=None):
		if self.channel.closed:
			print "channel was closed. getting new one."
			self.channel = self.ssh.get_root_ssh_channel()
			
		cron_ng_keys = ['ScalarizrMessaging', 'MessagingQueue', 'Scaling', 'Poller', 'BundleTasksManager']
		
		if not name in cron_ng_keys:
			raise Exception('Unknown cronjob %s' % name)
	
		cron_php_path = 'cron/cron.php' if 'BundleTasksManager' == name else 'cron-ng/cron.php'
		
		home_path = config.get('./scalr/home_path')
		self._logger.info('channel: %s' % type(self.channel))
		
		self._logger.info('cd %s' % home_path)
		execute(self.channel, 'cd ' + home_path)

		farm_str = ''
		if (self.farmid and name in ('ScalarizrMessaging', 'Scaling')):
			farm_str = ('--farm-id=%s' % self.farmid)
			
		elif server_id and name == 'BundleTasksManager':
			farm_str = ('--server-id=%s' % server_id)
			
		job_cmd = 'php -q ' + cron_php_path + ' --%s %s' % (name, farm_str)
		self._logger.info('Starting cronjob: %s' % job_cmd)

		out = execute(self.channel, job_cmd, 200)

		log_filename = name + time.strftime('_%d_%b_%H-%M') + '.log'
		try:
			fp = open(os.path.join(log_path, log_filename), 'w')
			fp.write(out)
			fp.close()
		except:
			pass
		return out
	
	def enable_svn_access(self, ip):
		if self.channel.closed:
			self.channel = self.ssh.get_root_ssh_channel()
			
		out = execute(self.channel, 'svn2allow %s' % ip)
		
		if not 'Successfully enabled SVN access' in out:
			raise Exception("Can't enable SVN access to %s. Output: \n%s" % (ip, out))
		
		