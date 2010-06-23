'''
Created on 14.06.2010

@author: spike
'''
from scalarizr.bus import bus
from scalarizr.behaviour import Behaviours
from scalarizr.handlers import Handler, HandlerError
from scalarizr.util import fstool, system, cryptotool, initd, disttool,\
		configtool, filetool
from distutils import version
from subprocess import Popen, PIPE, STDOUT
import logging, os, re, time, pexpect
import signal, pwd



if disttool.is_redhat_based():
	initd_script = "/etc/init.d/mysqld"
elif disttool.is_debian_based():
	initd_script = "/etc/init.d/mysql"
else:
	raise HandlerError("Cannot find MySQL init script. Make sure that mysql server is installed")

pid_file = None
try:
	out = system("my_print_defaults mysqld")
	m = re.search("--pid_file=(.*)", out, re.MULTILINE)
	if m:
		pid_file = m.group(1)
except:
	pass

# Register mysql service
logger = logging.getLogger(__name__)
logger.debug("Explore MySQL service to initd module (initd_script: %s, pid_file: %s)", initd_script, pid_file)
initd.explore("mysql", initd_script, pid_file)
print ""


OPT_ROOT_USER   		= "root_user"
OPT_ROOT_PASSWORD   	= "root_password"
OPT_REPL_USER   		= "repl_user"
OPT_REPL_PASSWORD   	= "repl_password"
OPT_STAT_USER   		= "stat_user"
OPT_STAT_PASSWORD   	= "stat_password"

def get_handlers ():
	return [MysqlHandler()]

class MysqlMessages:
	CREATE_DATA_BUNDLE = "Mysql_CreateDataBundle"
	CREATE_DATA_BUNDLE_RESULT = "Mysql_CreateDataBundleResult"
	CREATE_BACKUP = "Mysql_CreateBackup"
	CREATE_PMA_USER = "Mysql_CreatePmaUser"
	CREATE_PMA_USER_RESULT = "Mysql_CreatePmaUserResult"
	MASTER_UP = "Mysql_MasterUp"


class MysqlHandler(Handler):
	_logger = None
	_queryenv = None

	def __init__(self):
		self._logger = logging.getLogger(__name__)
		self._queryenv = bus.queryenv_service
		self._platform = bus.platform
		self._iid = self._platform.get_instance_id()
		bus.on("init", self.on_init)

	def on_init(self):
		bus.on("before_host_up", self.on_before_host_up)

	def on_before_host_up(self, message):
		config = bus.config
		role_name = config.get(configtool.SECT_GENERAL, 'role_name')
		role_params = self._queryenv.list_role_params(role_name)
		if role_params["mysql_data_storage_engine"]:
			# Poneslas' pizda po ko4kam
			if "master" == role_name:
				# Mount EBS
				self._init_storage()

				# If It's 1st init of mysql master
				if not os.path.exists('/mnt/mysql-data') and not os.path.exists('/mnt/mysql-misc'):
					print "########### Stopping mysql"
					self._stop_mysql()
					# Move datadir to EBS
					self._change_mysql_dir('log_bin', '/mnt/mysql-misc/binlog.log', 'mysqld')
					self._change_mysql_dir('datadir', '/mnt/mysql-data/', 'mysqld')
					
					print "########### Starting mysql"
					self._start_mysql()
					root_user = "scalarizr"
					repl_user = "scalarizr_repl"
					stat_user = "scalarizr_stat"

					root_password, repl_password, stat_password =  self._add_mysql_users(root_user, repl_user, stat_user)

					message.mysql_repl_user = repl_user
					message.mysql_repl_password = repl_password
					message.mysql_stat_user = stat_user
					message.mysql_stat_password = stat_password

					self._master_replication_init()
					
					update = role_params.get("create_ec2_snapshot", None)
					snap_id, log_file, log_pos = self._create_snapshot(root_user, root_password, update)

					if None != snap_id:
						message.snapshot_id = snap_id
					message.log_file	= log_file
					message.log_pos		= log_pos
				# If EBS volume had mysql dirs (N-th init)
				else:
					self._change_mysql_dir('log_bin', '/mnt/mysql-misc/binlog.log', 'mysqld')
					self._change_mysql_dir('datadir', '/mnt/mysql-data/', 'mysqld')
					self._master_replication_init()
					# Retrieve scalarizr's mysql username and password
					section			= configtool.get_behaviour_section_name(Behaviours.MYSQL)
					root_user		= config.get(section, OPT_ROOT_USER)
					root_password	= config.get(section, OPT_ROOT_PASSWORD)
					# Updating snapshot metadata
					snap_id, log_file, log_pos = self._create_snapshot(root_user, root_password)
					# Sending updated metadata to scalr
					message.log_file = log_file
					message.log_pos = log_pos
					
					self._start_mysql()
					
			elif "slave" == role_name or "eph" == role_params["mysql_data_storage_engine"]:
				try:
					devname = '/dev/' + self._platform.get_block_device_mapping()["ephemeral0"]
				except Exception, e:
					self._logger.error('Cannot retrieve device %s info: %s', devname, e)
					raise

				self._mount_device(devname)
					
					
	def _init_storage(self):
			vol_id = self._role_params["mysql_ebs_vol_id"]
			devname = '/dev/sdo'
			ec2connection = self._platform.new_ec2_conn()
			# Attach ebs
			ebs_volumes = ec2connection.get_all_volumes([vol_id])

			if 1 == len(ebs_volumes):
				self._ebs_volume = ebs_volumes[0]
				if self._ebs_volume.volume_state() == 'available':
					ec2connection.attach_volume(vol_id, self._iid, devname)
					while self._ebs_volume.attachment_state() != 'attached':
						time.sleep(5)
			else:
				self._logger.error('Can\'t find volume with ID =  %s ', vol_id)
				raise

			# Mount ebs # fstool.mount()
			self._mount_device(devname)

	def _start_mysql_skip_grant_tables(self):
		self._stop_mysql()
		
		if disttool.is_redhat_based():
			daemon = "/usr/libexec/mysqld"
		else:
			daemon = "/usr/sbin/mysqld"
		
		if os.path.exists(daemon) and os.access(daemon, os.X_OK):
			self._logger.info("Starting mysql server with --skip-grant-tables")
			myd = Popen([daemon, '--skip-grant-tables'], stdin=PIPE, stdout=PIPE, stderr=STDOUT)
		else:
			self._logger.error("MySQL daemon '%s' doesn't exists", daemon)
			return False
		time.sleep(1)
		return myd


	def _create_snapshot(self, root_user, root_password, update = None):
		self._start_mysql()
		# Lock tables
		sql = pexpect.spawn('/usr/bin/mysql -u' + root_user + ' -p' + root_password)
		#sql = pexpect.spawn('/usr/bin/mysql -uroot -p123')
		sql.expect('mysql>')
		sql.sendline('FLUSH TABLES WITH READ LOCK;\n')
		sql.expect('mysql>')
		sql.sendline('SHOW MASTER STATUS;\n')
		sql.expect('mysql>')
		sql.expect('mysql>')
		lines = sql.before
		# Retrieve log file and log position
		log_row = re.search(re.compile('^\|\s*([\w-]*\.\d*)\s*\|\s*(\d*)', re.MULTILINE), lines)
		if log_row:
			log_file = log_row.group(1)
			log_pos = log_row.group(2)
		else:
			log_file = log_pos = None

		# Creating e2 snapshot
		if update:
			snapshot = self._ebs_volume.create_snapshot()
			snap_id = snapshot.id
		else:
			snap_id = None
		
		sql.sendline('UNLOCK TABLES;\n')
		sql.close()

		return snap_id, log_file, log_pos

		# Sending snapshot data to scalr


	def _add_mysql_users(self, root_user, repl_user, stat_user):
		
		myd = self._start_mysql_skip_grant_tables()
		print "PID: ", myd.pid
		myclient = Popen(["/usr/bin/mysql"], stdin=PIPE, stdout=PIPE, stderr=PIPE)
		out,err = myclient.communicate('SELECT VERSION();')
		print "######## OUT:", out, err		
		mysql_ver_str = re.search(re.compile('\d*\.\d*\.\d*', re.MULTILINE), out).group(0)
		mysql_ver = version.StrictVersion(mysql_ver_str)
		
		if mysql_ver >= version.StrictVersion('5.1.6'):
			priv_count = 28
		else:
			priv_count = 26
			
		myclient = Popen(["/usr/bin/mysql"], stdin=PIPE, stdout=PIPE, stderr=PIPE)
		# Define users and passwords
		root_password, repl_password, stat_password = map(lambda x: re.sub('[^\w]','', cryptotool.keygen(20)), range(3))
		# Add users
#		sql = "INSERT INTO mysql.user VALUES('localhost','"+root_user+"',PASSWORD('"+root_password+"')" + ",'Y'"*priv_count + ",''"*4 +',0'*4+");"
		sql = "INSERT INTO mysql.user VALUES('%','"+root_user+"',PASSWORD('"+root_password+"')" + ",'Y'"*priv_count + ",''"*4 +',0'*4+");"
		sql += " INSERT INTO mysql.user (Host, User, Password, Repl_slave_priv) VALUES ('%','"+repl_user+"',PASSWORD('"+repl_password+"'),'Y');"
		sql += " INSERT INTO mysql.user (Host, User, Password, Repl_client_priv) VALUES ('%','"+stat_user+"',PASSWORD('"+stat_password+"'),'Y');"
		sql += " FLUSH PRIVILEGES;"
		out,err = myclient.communicate(sql)
		# Save root user to /etc/scalr/private.d/behaviour.mysql.ini
		conf_updates = {configtool.get_behaviour_section_name(Behaviours.MYSQL) : {
			OPT_ROOT_USER		: root_user,
			OPT_ROOT_PASSWORD	: root_password,
			OPT_REPL_USER		: repl_user,
			OPT_REPL_PASSWORD	: repl_password,
			OPT_STAT_USER		: stat_user,
			OPT_STAT_PASSWORD	: stat_password
		}}
		configtool.update(configtool.get_behaviour_filename(Behaviours.MYSQL, ret=configtool.RET_PRIVATE),
			conf_updates)
		os.kill(myd.pid, signal.SIGKILL)
		self._start_mysql()
		return (root_password, repl_password, stat_password)

	def _master_replication_init(self):

		# Create /etc/mysql if Hats
		if disttool.is_redhat_based():
			try:
				os.makedirs('/etc/mysql/')
			except OSError, e:
				self._logger.error('Couldn`t create directory /etc/mysql/: %s', e)

		# Writting replication config
		try:
			file = open('/etc/mysql/farm-replication.cnf', 'w')
		except IOError, e:
			self._logger.error('Cannot open /etc/mysql/farm-replication.cnf: %s', e.strerror )
			raise
		else:
			file.write('[mysqld]\nserver-id\t\t=\t1\nmaster-connect-retry\t\t=\t15\n')
			file.close()

		# Get my.cnf location
		if disttool.is_redhat_based():
			my_cnf_file = "/etc/my.cnf"
		else:
			my_cnf_file = "/etc/mysql/my.cnf"

		# Include farm-replication.cnf to my.cnf
		try:
			file = open(my_cnf_file, 'a+')
		except IOError, e:
			self._logger.error('Can\'t open %s: %s', my_cnf_file, e.strerror )
			raise
		else:
			my_cnf = file.read()
			if not re.search(re.compile('^!include \/etc\/mysql\/farm-replication\.cnf', re.MULTILINE), my_cnf):
				file.write('\n!include /etc/mysql/farm-replication.cnf\n')
		finally:
			file.close()
		self._stop_mysql()
		self._start_mysql()
			
				

	def _start_mysql(self):
		try:
			initd.start("mysql")
		except initd.InitdError, e:
			self._logger.error(e)

	def _stop_mysql(self):
		try:
			initd.stop("mysql")
		except initd.InitdError, e:
			logger.error(e)
			
	def _change_mysql_dir2(self, directive=None, value = None, section=None):
		if value and directive:
			pass

	def _change_mysql_dir(self, directive=None, dirname = None, section=None):
		# Locating mysql config file			
		if disttool.is_redhat_based():
			my_cnf_file = "/etc/my.cnf"
		else:
			my_cnf_file = "/etc/mysql/my.cnf"
		
		#Reading Mysql config file		
		try:
			file = open(my_cnf_file, 'r')
		except IOError, e:
			self._logger.error('Can\'t open %s: %s', my_cnf_file, e.strerror )
			raise
		else:
			myCnf = file.read()
			file.close
					
		# Retrieveing mysql user from passwd		
		mysql_user	= pwd.getpwnam("mysql")
		directory	= os.path.dirname(dirname)
		sectionrow	= re.compile('(.*)(\['+str(section)+'\])(.*)', re.DOTALL)
		search_row	= re.compile('(^\s*'+directive+'\s*=\s*)((/[\w-]+)+)[/\s](/[\n\w-]+\.\w+)?', re.MULTILINE)
		src_dir_row = re.search(search_row, myCnf)
		
		if src_dir_row:
			if not os.path.isdir(directory):
				src_dir = os.path.dirname(src_dir_row.group(2) + "/")
				if os.path.isdir(src_dir):
					self._logger.info('Copying mysql directory \'%s\' to \'%s\'', src_dir, directory)
					rsync = filetool.Rsync()
					rsync.source(src_dir)
					rsync.dest(directory)
					rsync.archive()
					print "######## RSYNC ROW ", rsync
					system(str(rsync))
					myCnf = re.sub(search_row, '\\1'+ dirname + '\n' , myCnf)
				else:
					self._logger.error('Mysql directory \'%s\' doesn\'t exist. Creating new in \'%s\'', src_dir, directory)
					myCnf = re.sub(search_row, '' , myCnf)
					os.makedirs(directory)
					regexp = re.search(sectionrow, myCnf)
					if regexp:
						myCnf = re.sub(sectionrow, '\\1\\2\n'+ directive + ' = ' + dirname + '\n\\3' , myCnf)
					else:
						myCnf += '\n' + directive + ' = ' + dirname
			else:
				myCnf = re.sub(search_row, '\\1'+ dirname + '\n' , myCnf)
		else:
			if not os.path.isdir(directory):
				os.makedirs(directory)
			regexp = re.search(sectionrow, myCnf)
			if regexp:
				myCnf = re.sub(sectionrow, '\\1\\2\n'+ directive + ' = ' +dirname + '\n\\3' , myCnf)
			else:
				myCnf += '\n' + directive + ' = ' + dirname
		
		# Setting new directory permissions
		try:
			os.chown(directory, mysql_user.pw_uid, mysql_user.pw_gid)
		except OSError, e:
			self._logger.error('Cannot chown Mysql directory %s', directory)
		
		# Writing new MySQL config
		file = open('/etc/mysql/my.cnf', 'w')
		file.write(myCnf)
		file.close()
		
		# Adding rules to apparmor config 
		if disttool.is_debian_based():
			try:
				file = open('/etc/apparmor.d/usr.sbin.mysqld', 'r')
			except IOError, e:
				pass
			else:
				app_rules = file.read()
				file.close()
				if not re.search (directory, app_rules):
					file = open('/etc/apparmor.d/usr.sbin.mysqld', 'w')
					app_rules = re.sub(re.compile('(\.*)(\})', re.S), '\\1\n'+directory+' r,\n'+'\\2', app_rules)
					app_rules = re.sub(re.compile('(\.*)(\})', re.S), '\\1'+directory+'* rwk,\n'+'\\2', app_rules)
					file.write(app_rules)
					file.close()
					initd.explore('apparmor', '/etc/init.d/apparmor')
					try:
						initd.reload('apparmor', True)
					except initd.InitdError, e:
						self._logger.error('Cant restart apparmor')
									

	def _mount_device(self, devname):
			fstab = fstool.Fstab()
			if None != devname:
					try:
						fstool.mount(devname, '/mnt', ["-t auto"])
					except fstool.FstoolError, e:
						if fstool.FstoolError.NO_FS == e.code:
							system("/sbin/mkfs.ext3 -F " + devname + " 2>&1")
							try:
								fstool.mount(devname, '/mnt', ["-t auto"])
							except fstool.FstoolError, e:
								raise
						else:
							raise

			if not fstab.contains(devname, rescan=True):
				self._logger.info("Adding a record to fstab")
				fstab.append(fstool.TabEntry(devname, '/mnt', "auto", "defaults\t0\t0"))
