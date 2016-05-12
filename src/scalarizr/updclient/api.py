'''
Created on Jan 23, 2012

@author: marat
'''

import binascii
import copy
import json
import logging
import os
import pprint
import re
import shutil
import sqlite3 as sqlite
import subprocess
import sys
import threading
import urllib2
import uuid
import time
import glob
import pkg_resources
import multiprocessing
from distutils.version import LooseVersion

from scalarizr.util import metadata
from scalarizr import linux
from scalarizr import queryenv
from scalarizr import rpc
from scalarizr import config
from scalarizr import __version__
from scalarizr.node import __node__
from scalarizr.api import operation
from scalarizr.api.binding import jsonrpc_http
from scalarizr.bus import bus
from scalarizr.linux import coreutils
from scalarizr.linux.pkgmgr import repository
from scalarizr.messaging.p2p.service import P2pMessageService
from scalarizr.util import initdv2, sqlite_server, wait_until

from scalarizr.updclient import pkgmgr

if linux.os.windows:
    import win32com
    import win32com.client
    from common.utils.winutil import coinitialized


LOG = logging.getLogger(__name__)
DATE_FORMAT = '%a %d %b %Y %H:%M:%S UTC'


class UpdateError(Exception):
    pass


class NoSystemUUID(Exception):
    pass


def norm_user_data(data):
    data['server_id'] = data.pop('serverid')
    data['messaging_url'] = data.pop('p2p_producer_endpoint')
    # - my uptime is 1086 days, 55 mins, o-ho-ho
    if data['messaging_url'] == 'http://scalr.net/messaging':
        data['messaging_url'] = 'https://my.scalr.com/messaging'
    if data['queryenv_url'] == 'http://scalr.net/query-env':
        data['queryenv_url'] = 'https://my.scalr.com/query-env'
    data['farm_role_id'] = data.pop('farm_roleid', None)
    return data


def value_for_repository(deb=None, rpm=None, win=None):
    if linux.os.windows:
        return win
    elif linux.os.redhat_family or linux.os.oracle_family:
        return rpm
    else:
        return deb


class UpdClientAPI(object):

    '''
    States:
     * noop - initial state
     * in-progress -  update performed
     * completed - new package installed
     * rollbacked - new package installation failed, so previous was restored
     * error - update failed and unrecovered

    Transitions:
     noop -> in-progress
     in-progress -> completed -> in-progress
     in-progress -> rollbacked -> in-progress
     in-progress -> error -> in-progress

    In-progress transitions:
     in-progress/prepare -> in-progress/check-allowed (when not force)
     in-progress/check-allowed -> in-progress/install
     in-progress/install -> completed/wait-ack -> completed
     in-progress/install -> in-progress/rollback -> rollbacked
    '''

    client_mode = 'client'
    api_port = 8008
    win_update_timeout = 300
    server_url = 'http://update.scalr.net/'
    repository = 'latest'
    repo_url = value_for_repository(
        deb='http://repo.scalr.net/apt latest main',
        rpm='http://repo.scalr.net/rpm/latest/rhel/$releasever/$basearch',
        win='http://repo.scalr.net/win/latest'
    )
    downgrades_enabled = True
    system_id_is_server_id = True  # [SCALARIZR-2129] flag for postinst migration

    server_id = farm_role_id = system_id = platform = queryenv_url = messaging_url = None
    scalr_id = scalr_version = None
    _state = prev_state = installed = candidate = executed_at = error = dist = None

    update_server = None
    messaging_service = None
    scalarizr = None
    queryenv = None
    pkgmgr = None
    daemon = None
    shutdown_ev = None

    system_matches = False

    etc_path = __node__['etc_dir']
    share_path = bus.share_path = __node__['share_dir']
    log_file = os.path.join(__node__['log_dir'], 'scalarizr_update.log')

    _private_path = os.path.join(etc_path, 'private.d')
    status_file = os.path.join(_private_path, 'update.status')
    win_status_file = os.path.join(_private_path, 'update_win.status')
    crypto_file = os.path.join(_private_path, 'keys', 'default')
    db_file = os.path.join(_private_path, 'db.sqlite')
    del _private_path

    @property
    def is_solo_mode(self):
        return self.client_mode == 'solo'

    @property
    def is_client_mode(self):
        return self.client_mode == 'client'

    @property
    def package_type(self):
        return 'omnibus' if os.path.isdir('/opt/scalarizr/embedded') else 'fogyish'

    @property
    def package(self):
        result = 'scalarizr'
        if not linux.os.windows:
            result += '-' + self.platform
        return result

    def deps(self, version):
        # Form the list of dependencies to install with the main package (self.package).
        if linux.os.windows:
            return None
        return [{'name': 'scalarizr', 'version': version}]

    def state():
        # pylint: disable=E0211, E0202
        def fget(self):
            return self._state

        def fset(self, state):
            if state == self._state:
                return
            self.prev_state = self._state
            self._state = state
            LOG.info('State transition: {0} -> {1}'.format(self.prev_state, state))
        return locals()
    state = property(**state())

    def __init__(self, **kwds):
        self._update_self_dict(kwds)
        self.pkgmgr = pkgmgr.create_pkgmgr(self.repo_url)
        self.daemon = initdv2.Daemon('scalarizr')
        self.op_api = operation.OperationAPI()
        self.dist = '{name} {release} {codename}'.format(**linux.os)
        self.state = 'noop'
        self.shutdown_ev = threading.Event()
        self.early_bootstrapped = False

    def _update_self_dict(self, data):
        self.__dict__.update(data)
        if 'state' in data:
            self.__dict__['_state'] = data['state']

    def _init_queryenv(self):
        LOG.debug('Initializing QueryEnv')
        queryenv_creds = (self.queryenv_url,
                          self.server_id,
                          self.crypto_file)
        self.queryenv = queryenv.new_queryenv(*queryenv_creds)
        self.queryenv.get_latest_version()  # check crypto key
        bus.queryenv_service = self.queryenv

    def _init_db(self):
        def connect_db():
            conn = sqlite.connect(self.db_file, 5.0)
            conn.row_factory = sqlite.Row
            conn.text_factory = sqlite.OptimizedUnicode
            return conn

        if not os.path.exists(self.db_file) or not os.stat(self.db_file).st_size:
            LOG.debug('Creating SQLite database')
            conn = connect_db()
            try:
                with open(os.path.join(self.share_path, 'db.sql')) as fp:
                    conn.executescript(fp.read())
                conn.commit()
            finally:
                conn.close()

        # Configure database connection pool
        LOG.debug('Initializing database connection')
        t = sqlite_server.SQLiteServerThread(connect_db)
        t.setDaemon(True)
        t.start()
        sqlite_server.wait_for_server_thread(t)
        bus.db = t.connection

    def _init_services(self):
        if not bus.db:
            self._init_db()

        if not bus.cnf:
            bus.cnf = config.ScalarizrCnf(self.etc_path)
            bus.cnf.bootstrap()

        if not self.queryenv:
            def init_queryenv():
                try:
                    self._init_queryenv()
                    return True
                except queryenv.InvalidSignatureError:
                    if bus.cnf.state == 'bootstrapping':
                        LOG.debug('Ignore InvalidSignatureError while Scalarizr is bootstrapping, retrying...')
                        return False
                    else:
                        raise
            wait_until(init_queryenv, timeout=120, sleep=10)

        if not self.messaging_service:
            LOG.debug('Initializing messaging')
            bus.messaging_service = P2pMessageService(
                server_id=self.server_id,
                crypto_key_path=self.crypto_file,
                producer_url=self.messaging_url,
                producer_retries_progression='1,2,5,10,20,30,60')

        if self.is_client_mode and not self.update_server:
            self.update_server = jsonrpc_http.HttpServiceProxy(self.server_url, self.crypto_file,
                                                               server_id=self.server_id,
                                                               sign_only=True)

        if not self.scalarizr:
            self.scalarizr = jsonrpc_http.HttpServiceProxy('http://localhost:8010/', self.crypto_file)

    def get_system_id(self):
        return metadata.user_data()['serverid']

    def obsolete_get_system_id(self):
        if linux.os.windows:
            @coinitialized
            def win32_serial_number():
                try:
                    wmi = win32com.client.GetObject('winmgmts:')
                    for row in wmi.ExecQuery('SELECT SerialNumber FROM Win32_BIOS'):
                        return row.SerialNumber
                    else:
                        LOG.debug('WMI returns empty UUID')
                except:
                    LOG.debug('WMI query failed: %s', sys.exc_info()[1])
        else:
            def dmidecode_uuid():
                try:
                    ret = linux.system('dmidecode -s system-uuid', shell=True)[0].strip()
                    if not ret:
                        LOG.debug('dmidecide returns empty UUID')
                    elif len(ret) != 36:
                        LOG.debug("dmidecode returns invalid UUID: %s", ret)
                    else:
                        return ret
                except:
                    LOG.debug('dmidecode failed: %s', sys.exc_info()[1])

        LOG.info('Getting System ID (obsolete)')
        ret = win32_serial_number() if linux.os.windows else dmidecode_uuid()
        if not ret:
            ret = metadata.instance_id()
        if not ret:
            raise NoSystemUUID('System UUID not detected')
        return ret


    def _ensure_repos(self):
        if not linux.os.windows_family:
            repo = repository('scalr-{0}'.format(self.repository), self.repo_url)
            # Delete previous repository
            for filename in glob.glob(os.path.dirname(repo.filename) + os.path.sep + 'scalr*'):
                if os.path.isfile(filename):
                    os.remove(filename)
            # Ensure new repository
            repo.ensure()

    def bootstrap(self, dry_run=False):
        # [SCALARIZR-1797]
        # Cleanup RPMDb from old entries
        if linux.os.redhat_family and not dry_run:
            out = linux.system(('rpm', '-qa', 'scalarizr*', '--queryformat', '%{NAME}|%{VERSION}\n'))[0]
            for line in out.strip().split('\n'):
                name, version = line.strip().split('|')
                if not version == __version__:
                    linux.system(('rpm', '-e', '--nodeps', '--justdb', '{0}-{1}'.format(name, version)))
        try:
            LOG.info('Getting System ID')
            self.system_id = self.get_system_id()
            LOG.info('System ID: {}'.format(self.system_id))
        except:
            # This will force updclient to perform check updates each startup,
            # this is the optimal behavior cause that's ensure latest available package
            LOG.debug('Unable to get System ID: %s', sys.exc_info()[1])
            self.system_id = str(uuid.uuid4())
            LOG.debug('System ID (random generated): {}'.format(self.system_id))

        system_matches = False
        status_data = None
        if os.path.exists(self.status_file):
            LOG.debug('Checking %s', self.status_file)
            with open(self.status_file) as fp:
                status_data = json.load(fp)
                if 'downgrades_enabled' not in status_data:
                    # Field introduced in 2.7.12
                    # Missing field here means downgrades_enabled=False,
                    # cause it's setted by postinst migration to new update system
                    status_data['downgrades_enabled'] = False
            system_matches = status_data['system_id'] == self.system_id
            if not system_matches:
                if not status_data.get('system_id_is_server_id') \
                    and status_data['system_id'] == self.obsolete_get_system_id():
                    LOG.info('System-ID (obsolete) in status file matches current one')
                    status_data['system_id'] = self.system_id
                    status_data['system_id_is_server_id'] = True
                    system_matches = True
                else:
                    LOG.info('System ID changed: %s => %s',
                             status_data['system_id'], self.system_id)
            else:
                LOG.debug('System-ID in status file matches current one')
        else:
            LOG.debug('Status file %s not exists', self.status_file)

        if system_matches:
            LOG.info('Reading state from %s', self.status_file)
            self._update_self_dict(status_data)

        if not system_matches:
            LOG.info('Initializing UpdateClient...')
            user_data = copy.deepcopy(metadata.user_data())
            norm_user_data(user_data)
            LOG.info('Applying configuration from user-data')
            self._update_self_dict(user_data)

            crypto_dir = os.path.dirname(self.crypto_file)
            if not os.path.exists(crypto_dir):
                os.makedirs(crypto_dir)
            if os.path.exists(self.crypto_file):
                LOG.info('Testing that crypto key works (file: %s)', self.crypto_file)
                try:
                    self._init_db()
                    self._init_queryenv()
                    LOG.info('Crypto key works')
                except queryenv.InvalidSignatureError:
                    LOG.info("Crypto key doesn't work: got invalid signature error")
                    self.queryenv = None
            if not self.queryenv:
                LOG.info("Use crypto key from user-data")
                if os.path.exists(self.crypto_file):
                    os.chmod(self.crypto_file, 0600)
                with open(self.crypto_file, 'w+') as fp:
                    fp.write(user_data['szr_key'])
                os.chmod(self.crypto_file, 0400)
        self.early_bootstrapped = True

        try:
            self._init_services()
        except sqlite.OperationalError as e:
            # [SCALARIZR-2168] Scalarizr as part of post rebundle cleanup removed db.sqlite
            LOG.debug('Database error: {}'.format(e))
            wait_until(
                lambda: os.path.exists(self.db_file) and os.stat(self.db_file).st_size,
                timeout=10, sleep=0.1)
            self._init_db()
            self._init_services()

        # - my uptime is 644 days, 20 hours and 13 mins and i know nothing about 'platform' in user-data
        if not self.platform:
            self.platform = bus.cnf.rawini.get('general', 'platform')
        # - my uptime is 1086 days, 55 mins and i know nothing about 'farm_roleid' in user-data
        if not self.farm_role_id:
            self.farm_role_id = bus.cnf.rawini.get('general', 'farm_role_id')

        self.system_matches = system_matches
        if not self.system_matches:
            if dry_run:
                self._sync()
            else:
                self.update(bootstrap=True)
        else:
            # if self.state in ('completed/wait-ack', 'noop'):
            if self.state not in ('error', 'rollbacked'):
                # forcefully finish any in-progress operations
                self.state = 'completed'
            self.store()

            # Set correct repo value at start. Need this to recover in case they
            # were not set during _sync() in the previous run [SCALARIZR-1885]
            self._ensure_repos()
        if not (self.shutdown_ev.is_set() or dry_run or
                (self.state == 'error' and not system_matches) or
                self.daemon.running):
            # we shouldn't start Scalarizr
            # - when UpdateClient is terminating
            # - when UpdateClient is not performing any updates
            # - when state is 'error' and it's a first UpdateClient start on a new system
            # - when Scalarizr is already running
            self.daemon.start()
        if self.state == 'completed/wait-ack':
            obsoletes = pkg_resources.Requirement.parse('A<=2.7.5')
            inst = re.sub(r'^\d\:', '', self.installed)  # remove debian epoch
            if inst in obsoletes:
                LOG.info('UpdateClient is going to restart itself, cause ')

                def restart_self():
                    time.sleep(5)
                    name = 'ScalrUpdClient' if linux.os.windows else 'scalr-upd-client'
                    service = initdv2.Daemon(name)
                    service.restart()
                proc = multiprocessing.Process(target=restart_self)
                proc.start()

    def _ensure_daemon(self):
        LOG.debug('Checking {0} status...'.format(self.daemon.name))
        if not self.daemon.running:
            LOG.debug('{0} is not running'.format(self.daemon.name))
            self.daemon.start()
            LOG.debug('{0} started successfully'.format(self.daemon.name))
        else:
            LOG.debug('{0} is running. '.format(self.daemon.name))

    def _sync(self):
        LOG.info('Syncing configuration from Scalr')
        params = self.queryenv.list_farm_role_params(self.farm_role_id)
        update = params.get('params', {}).get('base', {}).get('update', {})
        self._update_self_dict(update)
        self.repo_url = value_for_repository(
            deb=update.get('deb_repo_url', ''),
            rpm=update.get('rpm_repo_url', ''),
            win=update.get('win_repo_url', '')
        )
        self.pkgmgr = pkgmgr.create_pkgmgr(self.repo_url)
        if self.repo_url.strip() == '':
            raise ValueError(
                'Got an empty repository URL. '
                'Check repositories configuration in Scalr config.yml')
        self.pkgmgr.updatedb()

        # Need this if user wants to perform a manual update [SCALARIZR-1885]
        self._ensure_repos()

        globs = self.queryenv.get_global_config()['params']
        self.scalr_id = globs['scalr.id']
        self.scalr_version = globs['scalr.version']

    @rpc.command_method
    def update(self, force=False, bootstrap=False, async=False, **kwds):
        # pylint: disable=R0912
        if bootstrap:
            force = True
            downgrades_enabled = self.downgrades_enabled
        else:
            downgrades_enabled = False
        reports = self.is_client_mode and not bootstrap

        def check_allowed():
            self.state = 'in-progress/check-allowed'

            if self.daemon.running and self.scalarizr.operation.has_in_progress():
                msg = ('Update denied ({0}={1}), '
                       'cause Scalarizr is performing log-term operation').format(
                           self.package, self.candidate)
                raise UpdateError(msg)

            if self.is_client_mode:
                try:
                    ok = self.update_server.update_allowed(
                        package=self.package,
                        version=self.candidate,
                        server_id=self.server_id,
                        scalr_id=self.scalr_id,
                        scalr_version=self.scalr_version)

                except urllib2.URLError:
                    raise UpdateError('Update server is down for maintenance')
                if not ok:
                    msg = ('Update denied ({0}={1}), possible issues detected in '
                           'later version. Blocking all upgrades until Scalr support '
                           'overrides.').format(
                               self.package, self.candidate)
                    raise UpdateError(msg)

        def do_update(op):
            self.executed_at = time.strftime(DATE_FORMAT, time.gmtime())
            self.state = 'in-progress/prepare'
            self.error = ''
            pkgmgr.LOG.addHandler(op.logger.handlers[0])

            try:
                self._sync()
                pkginfo = self.pkgmgr.status(self.package)
                if not pkginfo.get('candidate'):
                    max_available = pkginfo['available'][-1]
                    if LooseVersion(max_available or '0') == LooseVersion(pkginfo.get('installed') or '0'):
                        self.state = 'completed'
                        LOG.info('No new version available ({0})'.format(self.package))
                        return
                    if downgrades_enabled:
                        pkginfo['candidate'] = max_available
                    else:
                        self.state = 'completed'
                        LOG.info('New version {0!r} less then installed {1!r}, but downgrades disabled'.format(
                            max_available, pkginfo['installed']))
                        return
                self._update_self_dict(pkginfo)

                if not force:
                    check_allowed()

                try:
                    self.state = 'in-progress/install'

                    # [SCALARIZR-2165] handle downgrade on a system_id pre-migration package
                    if pkginfo['candidate'] in pkg_resources.Requirement.parse('A<=4.5.3'):
                        LOG.info('Updating System-ID with a pre-migration value (because of a package downgrade)')
                        self.system_id = self.obsolete_get_system_id()
                        self.system_id_is_server_id = False

                    self.store()
                    LOG.info('Installing {0}={1}'.format(
                        self.package, pkginfo['candidate']))

                    backup = pkginfo.get('installed_hash')
                    if not bootstrap:
                        if not backup and pkginfo['installed'] in pkginfo['available']:
                            # If we don't have the installed version in our cache it means user installed it by hand.
                            LOG.info(
                                "Couldn't find backup. Manually fetching currently installed packages to our cache")
                            backup = self.pkgmgr.fetch(
                                self.package, pkginfo['installed'], deps=self.deps(pkginfo['installed']))
                        if not backup:
                            LOG.warn(
                                "Couldn't find backup and couldn't fetch currently installed packages (they were installed from another repo?)")

                    hash = self.pkgmgr.fetch(self.package, self.candidate, deps=self.deps(self.candidate))
                    try:
                        self.pkgmgr.install(hash)
                        self._ensure_daemon()
                    except:
                        if backup:
                            # TODO: remove stacktrace
                            LOG.warn('Install failed, rollbacking. Error: %s',
                                     sys.exc_info()[1], exc_info=sys.exc_info())
                            self.state = 'in-progress/rollback'
                            self.error = str(sys.exc_info()[1])
                            self.pkgmgr.install(backup)
                            self._ensure_daemon()
                            self.state = 'rollbacked'
                            LOG.info('Rollbacked')
                            if reports:
                                self.report(False)
                        else:
                            raise
                    else:
                        self.state = 'completed/wait-ack'
                        self.installed = self.candidate
                        self.candidate = None
                        if reports:
                            self.report(True)
                    return self.status(cached=True)

                except KeyboardInterrupt:
                    if not linux.os.windows:
                        op.cancel()
                    return
                except:
                    if reports:
                        self.report(False)
                    raise
            except:
                e = sys.exc_info()[1]
                self.error = str(e)
                self.state = 'error'
                if isinstance(e, UpdateError):
                    op.logger.warn(str(e))
                    return self.status(cached=True)
                else:
                    raise
            finally:
                if not self.shutdown_ev.is_set():
                    self.store()
                pkgmgr.LOG.removeHandler(op.logger.handlers[0])

        return self.op_api.run('scalarizr.update', do_update, async=async,
                               exclusive=True, notifies=False)

    def shutdown(self):
        if self.early_bootstrapped:
            self.store()
        self.shutdown_ev.set()

    def store(self, status=None):
        status = status or self.status(cached=True)
        coreutils.mkdir(os.path.dirname(self.status_file), 0700)
        with open(self.status_file, 'w+') as fp:
            LOG.debug('Saving status: %s', pprint.pformat(status))
            json.dump(status, fp)

    def report(self, ok):
        if not self.is_client_mode:
            LOG.debug('Reporting is not enabled in {0} mode'.format(self.client_mode))
            return
        error = str(sys.exc_info()[1]) if not ok else ''
        self.update_server.report(
            ok=ok, package=self.package, version=self.candidate or self.installed,
            server_id=self.server_id, scalr_id=self.scalr_id, scalr_version=self.scalr_version,
            phase=self.state, dist=self.dist, error=error, package_type=self.package_type)

    @rpc.command_method
    def restart(self, force=False):
        getattr(self.daemon, 'forcerestart' if force else 'restart')()
        if not self.daemon.running:
            raise Exception('Service restart failed')

    @rpc.query_method
    def status(self, cached=False):
        status = {}
        keys_to_copy = [
            'server_id', 'farm_role_id', 'system_id', 'platform', 'queryenv_url',
            'messaging_url', 'scalr_id', 'scalr_version',
            'repository', 'repo_url', 'package', 'downgrades_enabled', 'system_id_is_server_id',
            'executed_at', 'state', 'prev_state', 'error', 'dist', 'package_type'
        ]

        pkginfo_keys = ['candidate', 'installed']
        if cached:
            keys_to_copy.extend(pkginfo_keys)
        else:
            self._sync()
            pkginfo = self.pkgmgr.status(self.package)
            status.update((key, pkginfo[key]) for key in pkginfo_keys)

        for key in keys_to_copy:
            status[key] = getattr(self, key)

        # we should exclude status from realtime data,
        # cause postinst for < 2.7.7 calls --make-status-file that fails to call scalarizr status
        #
        # \_ /bin/bash /etc/rc3.d/S84scalarizr_update start
        #     \_ /usr/bin/python2.6 -c ?from upd.client.package_mgr import YumPackageMgr?mgr = YumPackageMgr()?try:??mgr.u
        #         \_ /usr/bin/python /usr/bin/yum -d0 -y --disableplugin=priorities install scalarizr-base-2.7.28-1.el6 sc
        #             \_ /bin/sh /var/tmp/rpm-tmp.OXe7Fi 2
        #                 \_ /usr/bin/python2.6 -m scalarizr.updclient.app --make-status-file --downgrades-disabled
        #                     \_ /usr/bin/python2.6 /usr/bin/scalarizr status
        #                         \_ /usr/bin/python2.6 /usr/bin/scalr-upd-client status
        #                             \_ /usr/bin/python /usr/bin/yum -d0 -y clean expire-cache --exclude *.i386 --exclude
        if not cached:
            status['service_status'] = 'running' if self.daemon.running else 'stopped'
        else:
            status['service_status'] = 'unknown'
        status['service_version'] = __version__
        return status

    @rpc.service_method
    def execute(self, command=None):
        out, err, ret = linux.system(command, shell=True)
        return {
            'stdout': out,
            'stderr': err,
            'return_code': ret
        }

    @rpc.service_method
    def put_file(self, name=None, content=None, makedirs=False):
        if not re.search(r'^([A-Za-z0-9+/]{4})*([A-Za-z0-9+/]{4}|'
                         '[A-Za-z0-9+/]{3}=|[A-Za-z0-9+/]{2}==)\n?$', content):
            raise ValueError('File content is not a valid BASE64 encoded string')

        content = binascii.a2b_base64(content)

        directory = os.path.dirname(name)
        if makedirs and not os.path.exists(directory):
            os.makedirs(directory)

        tmpname = '%s.tmp' % name
        try:
            with open(tmpname, 'w') as dst:
                dst.write(content)
            shutil.move(tmpname, name)
        except:
            if os.path.exists(tmpname):
                os.remove(tmpname)
            raise
