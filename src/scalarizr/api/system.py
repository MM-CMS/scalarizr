"""
Created on Nov 25, 2011
@author: marat
"""

import binascii
import glob
import logging
from multiprocessing import pool
import os
import platform
import re
import signal
import subprocess
import threading
import time
import weakref

from scalarizr import rpc
from scalarizr import handlers
from scalarizr.api import operation as operation_api
from scalarizr.bus import bus
from scalarizr.handlers import script_executor
from scalarizr.handlers.chef import ChefClient
from scalarizr.handlers.chef import ChefSolo
from scalarizr.linux import mount
from scalarizr.node import __node__
from scalarizr.queryenv import ScalingMetric
from scalarizr.util import kill_childs
from scalarizr.util import Singleton
from scalarizr.messaging import Messages
from scalarizr.messaging.p2p.store import P2pMessageStore

from common.utils.facts import fact
from agent.tasks import sys as sys_tasks
from common.utils import sysutil
if fact['os']['name'] != 'windows':
    import augeas
else:
    from win32com import client
    from common.utils.winutil import coinitialized

LOG = logging.getLogger(__name__)


max_log_size = 5*1024*1024


class _ScalingMetricStrategy(object):
    """Strategy class for custom scaling metric"""

    @staticmethod
    def _get_execute(metric):
        if not os.access(metric.path, os.X_OK):
            raise BaseException("File is not executable: '%s'" % metric.path)

        exec_timeout = 3
        close_fds = fact['os']['name'] != 'windows'
        proc = subprocess.Popen(
            metric.path,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            close_fds=close_fds)

        timeout_time = time.time() + exec_timeout
        while time.time() < timeout_time:
            if proc.poll() is None:
                time.sleep(0.2)
            else:
                break
        else:
            kill_childs(proc.pid)
            if hasattr(proc, 'terminate'):
                # python >= 2.6
                proc.terminate()
            else:
                os.kill(proc.pid, signal.SIGTERM)
            raise BaseException('Timeouted')

        stdout, stderr = proc.communicate()

        if proc.returncode > 0:
            raise BaseException(stderr if stderr else 'exitcode: %d' % proc.returncode)

        return stdout.strip()


    @staticmethod
    def _get_read(metric):
        try:
            with open(metric.path, 'r') as fp:
                value = fp.readline()
        except IOError:
            raise BaseException("File is not readable: '%s'" % metric.path)

        return value.strip()


    @staticmethod
    def get(metric):
        error = ''
        try:
            if metric.retrieve_method == ScalingMetric.RetriveMethod.EXECUTE:
                value = _ScalingMetricStrategy._get_execute(metric)
            elif metric.retrieve_method == ScalingMetric.RetriveMethod.READ:
                value = _ScalingMetricStrategy._get_read(metric)
            else:
                raise BaseException('Unknown retrieve method %s' % metric.retrieve_method)
            try:
                value = float(value)
            except:
                raise ValueError("Can't convert metric value to float: {!r}".format(value))
        except (BaseException, Exception), e:
            value = 0.0
            error = str(e)[0:255]

        return {'id':metric.id, 'name':metric.name, 'value':value, 'error':error}


class SystemAPI(object):
    """
    Pluggable API to get system information similar to SNMP, Facter(puppet), Ohai(chef).
    Namespace::
        system
    """

    __metaclass__ = Singleton

    _HOSTNAME = '/etc/hostname'
    _DISKSTATS = '/proc/diskstats'
    _PATH = ['/usr/bin/', '/usr/local/bin/']
    _CPUINFO = '/proc/cpuinfo'
    _NETSTATS = '/proc/net/dev'
    _LOG_FILE = '/var/log/scalarizr.log'
    _DEBUG_LOG_FILE = '/var/log/scalarizr_debug.log'
    _UPDATE_LOG_FILE = '/var/log/scalarizr_update.log'
    _CENTOS_NETWORK_CFG = '/etc/sysconfig/network'

    def __init__(self):
        self._op_api = operation_api.OperationAPI()


    def _readlines(self, path):
        with open(path, "r") as fp:
            return fp.readlines()


    def add_extension(self, extension):
        """
        :type extension: object
        :param extension: Object with some callables to extend SysInfo public interface
        Note::
            Duplicates are resolved by overriding old function with a new one
        """

        for name in dir(extension):
            attr = getattr(extension, name)
            if not name.startswith('_') and callable(attr):
                if hasattr(self, name):
                    LOG.warn('Duplicate attribute %s. Overriding %s with %s',
                            name, getattr(self, name), attr)
                setattr(self, name, attr)


    @rpc.query_method
    def call_auth_shutdown_hook(self):
        """
        .. warning::
            Deprecated.
        """
        script_path = '/usr/local/scalarizr/hooks/auth-shutdown'
        LOG.debug("Executing %s" % script_path)
        if os.access(script_path, os.X_OK):
            return subprocess.Popen(script_path, stdout=subprocess.PIPE,
                stderr=subprocess.PIPE, close_fds=True).communicate()[0].strip()
        else:
            raise Exception('File not exists: %s' % script_path)


    @rpc.command_method
    def force_resume(self):
        """Forces init after reboot.

        Works only when last RebootFinish date is greater then uptime,
        there is no such message in history.
        """
        ln = bus.messaging_service.get_consumer().listeners[0]
        lifecycle = [hdlr \
            for hdlr in ln.get_handlers_chain() \
            if hdlr.__class__.__module__ == 'scalarizr.handlers.lifecycle' and \
                    hdlr.__class__.__name__ == 'LifeCycleHandler'][0]

        LOG.info('Scalarizr resumed after reboot (forced!)')
        lifecycle._start_after_reboot()
        return True


    @rpc.command_method
    def reboot(self):
        sysutil.reboot()

    @rpc.command_method
    def set_hostname(self, hostname=None):
        """
        Updates server's FQDN.
        :param str hostname: Fully Qualified Domain Name to set for this host
        Example::
            api.system.set_hostname(hostname = "myhostname.com")
        """
        sys_tasks.set_hostname(hostname, reboot=False)

    @rpc.query_method
    def get_hostname(self):
        """
        :return: server's FQDN.
        :rtype: list
        Example::
            "ec2-50-19-134-77.compute-1.amazonaws.com"
        """
        return fact['hostname']

    @rpc.query_method
    def block_devices(self):
        """
        :return: List of block devices including ramX and loopX
        :rtype: list
        """
        return fact['block_devices']


    @rpc.query_method
    def uname(self):
        """
        :return: general system information.
        :rtype: dict

        Example::
            {'kernel_name': 'Linux',
            'kernel_release': '2.6.41.10-3.fc15.x86_64',
            'kernel_version': '#1 SMP Mon Jan 23 15:46:37 UTC 2012',
            'nodename': 'marat.office.webta',
            'machine': 'x86_64',
            'processor': 'x86_64',
            'hardware_platform': 'x86_64'}
        """

        uname = platform.uname()
        return {
            'kernel_name': uname[0],
            'nodename': uname[1],
            'kernel_release': uname[2],
            'kernel_version': uname[3],
            'machine': uname[4],
            'processor': uname[5],
            'hardware_platform': fact['os']['arch']
        }

    @rpc.query_method
    def uptime(self):
        with open('/proc/uptime') as fp:
            return float(fp.read().strip().split()[0])

    @rpc.query_method
    def dist(self):
        """
        :return: Linux distribution info.
        :rtype: dict
        Example::
            {'distributor': 'ubuntu',
            'release': '12.04',
            'codename': 'precise'}
        """
        return {
            'distributor': fact['os']['name'],
            'release': str(fact['os']['release']),
            'codename': fact['os']['codename']
        }

    @rpc.query_method
    def cpu_info(self):
        """
        :return: CPU info from /proc/cpuinfo
        :rtype: list
        Example::
            [
               {
                  "bogomips":"5319.98",
                  "hlt_bug":"no",
                  "fpu_exception":"yes",
                  "stepping":"10",
                  "cache_alignment":"64",
                  "clflush size":"64",
                  "microcode":"0xa07",
                  "coma_bug":"no",
                  "cache size":"6144 KB",
                  "cpuid level":"13",
                  "fpu":"yes",
                  "model name":"Intel(R) Xeon(R) CPU           E5430  @ 2.66GHz",
                  "address sizes":"38 bits physical, 48 bits virtual",
                  "f00f_bug":"no",
                  "cpu family":"6",
                  "vendor_id":"GenuineIntel",
                  "wp":"yes",
                  "fdiv_bug":"no",
                  "power management":"",
                  "flags":"fpu tsc msr pae cx8",
                  "model":"23",
                  "processor":"0",
                  "cpu MHz":"2659.994"
               }
            ]
        """
        return fact['cpu']


    @rpc.query_method
    def cpu_stat(self):

        """
        :return: CPU stat from /proc/stat.
        :rtype: dict

        Example::
            {
                'user': 8416,
                'nice': 0,
                'system': 6754,
                'idle': 147309
            }
        """
        return fact['stat']['cpu']


    @rpc.query_method
    def mem_info(self):
        """
        :return: Memory information from /proc/meminfo.
        :rtype: dict

        Example::
             {
                'total_swap': 0,
                'avail_swap': 0,
                'total_real': 604364,
                'total_free': 165108,
                'shared': 168,
                'buffer': 17832,
                'cached': 316756
            }
        """
        return fact['stat']['memory']


    @rpc.query_method
    def load_average(self):
        """
        :return: Load average (1, 5, 15) in 3 items list.
        :rtype: list
        Example::
            [
               0.0,      // LA1
               0.01,     // LA5
               0.05      // LA15
            ]
        """
        return fact['stat']['load_average']


    @rpc.query_method
    def disk_stats(self):
        """
        :return: Disks I/O statistics.
        Data format::
            {
            <device>: {
                <read>: {
                    <num>: total number of reads completed successfully
                    <sectors>: total number of sectors read successfully
                    <bytes>: total number of bytes read successfully
                }
                <write>: {
                    <num>: total number of writes completed successfully
                    <sectors>: total number of sectors written successfully
                    <bytes>: total number of bytes written successfully
                },
            ...
            }
        See more at http://www.kernel.org/doc/Documentation/iostats.txt
        """
        return fact['stat']['disk']


    @rpc.query_method
    def net_stats(self):
        """
        :return: Network I/O statistics.
        Data format::
            {
                <iface>: {
                    <receive>: {
                        <bytes>: total received bytes
                        <packets>: total received packets
                        <errors>: total receive errors
                    }
                    <transmit>: {
                        <bytes>: total transmitted bytes
                        <packets>: total transmitted packets
                        <errors>: total transmit errors
                    }
                },
                ...
            }
        """
        return fact['stat']['net']


    @rpc.query_method
    def statvfs(self, mpoints=None):
        """
        :return: Information about available mounted file systems (total size and free space).
        Request::
            {
                "mpoints": [
                    "/mnt/dbstorage",
                    "/media/mpoint",
                    "/non/existing/mpoint"
                ]
            }
        Response::
            {
               "/mnt/dbstorage": {
                 "total" : 10000,
                 "free" : 5000
               },
               "/media/mpoint": {
                 "total" : 20000,
                 "free" : 1000
               },
               "/non/existing/mpoint" : null
            }
        """
        if not isinstance(mpoints, list):
            raise Exception('Argument "mpoints" should be a list of strings, '
                        'not %s' % type(mpoints))

        res = dict()
        mounts = mount.mounts()
        for mpoint in mpoints:
            try:
                assert mpoint in mounts
                mpoint_stat = os.statvfs(mpoint)
                res[mpoint] = dict()
                res[mpoint]['total'] = (mpoint_stat.f_bsize * mpoint_stat.f_blocks) / 1024  # Kb
                res[mpoint]['free'] = (mpoint_stat.f_bsize * mpoint_stat.f_bavail) / 1024   # Kb
            except:
                res[mpoint] = None

        return res


    @rpc.query_method
    def mounts(self):
        skip_mpoint_re = re.compile(r'/(sys|proc|dev|selinux)')
        skip_fstype = ('tmpfs', 'devfs')
        ret = {}
        for m in mount.mounts():
            if m.mpoint and (not (skip_mpoint_re.search(m.mpoint) or m.fstype in skip_fstype)):
                entry = m._asdict()
                entry.update(self.statvfs([m.mpoint])[m.mpoint])
                ret[m.mpoint] = entry
        return ret


    @rpc.command_method
    def scaling_metrics(self):
        """
        :return: list of scaling metrics
        :rtype: list

        Example::
            [{
                'id': 101011,
                'name': 'jmx.scaling',
                'value': 1,
                'error': None
            }, {
                'id': 202020,
                'name': 'app.poller',
                'value': None,
                'error': 'Couldnt connect to host'
            }]
        """

        # Obtain scaling metrics from Scalr.
        scaling_metrics = bus.queryenv_service.get_scaling_metrics()
        if not scaling_metrics:
            return []

        if not hasattr(threading.current_thread(), '_children'):
            threading.current_thread()._children = weakref.WeakKeyDictionary()

        wrk_pool = pool.ThreadPool(processes=10)

        try:
            return wrk_pool.map_async(_ScalingMetricStrategy.get, scaling_metrics).get()
        finally:
            wrk_pool.close()
            wrk_pool.join()


    @rpc.command_method
    def execute_scripts(self, scripts=None, global_variables=None, event_name=None,
            role_name=None, msg_body=None, async=False):
        def do_execute_scripts(op):
            msg = lambda: None
            msg.name = event_name
            msg.role_name = role_name
            msg.body = msg_body or {}
            msg.body.update({
                'scripts': scripts or [],
                'global_variables': global_variables or []
            })
            hdlr = script_executor.get_handlers()[0]
            hdlr(msg)

        return self._op_api.run('system.execute_scripts', do_execute_scripts, async=async)


    @rpc.query_method
    def get_script_logs(self, exec_script_id, maxsize=max_log_size):
        '''
        :return: stdout and stderr scripting logs
        :rtype: dict(stdout: base64encoded, stderr: base64encoded)
        '''
        stdout_match = glob.glob(os.path.join(
            script_executor.logs_dir,
            '*%s-out.log' % exec_script_id))
        stderr_match = glob.glob(os.path.join(
            script_executor.logs_dir,
            '*%s-err.log' % exec_script_id))

        err_rotated = ('Log file already rotatated and no more exists on server. '
                    'You can increase "Rotate scripting logs" setting under "Advanced" tab'
                    ' in Farm Designer')

        if not stdout_match:
            stdout = binascii.b2a_base64(err_rotated)
        else:
            stdout_path = stdout_match[0]
            stdout = binascii.b2a_base64(script_executor.get_truncated_log(stdout_path))
        if not stderr_match:
            stderr = binascii.b2a_base64(err_rotated)
        else:
            stderr_path = stderr_match[0]
            stderr = binascii.b2a_base64(script_executor.get_truncated_log(stderr_path))

        return dict(stdout=stdout, stderr=stderr)

    @rpc.query_method
    def get_debug_log(self):
        """
        :return: scalarizr debug log (/var/log/scalarizr.debug.log on Linux)
        :rtype: str
        """
        return binascii.b2a_base64(_get_log(self._DEBUG_LOG_FILE, -1))

    @rpc.query_method
    def get_update_log(self):
        """
        :return: scalarizr update log (/var/log/scalarizr.update.log on Linux)
        :rtype: str
        """
        return binascii.b2a_base64(_get_log(self._UPDATE_LOG_FILE, -1))

    @rpc.query_method
    def get_log(self):
        """
        :return: scalarizr info log (/var/log/scalarizr.log on Linux)
        :rtype: str
        """
        return binascii.b2a_base64(_get_log(self._LOG_FILE, -1))


def _get_log(logfile, maxsize=max_log_size):
    if maxsize != -1 and (os.path.getsize(logfile) > maxsize):
        return 'Unable to fetch Log file %s: file is larger than %s bytes' % (logfile, maxsize)
    try:
        with open(logfile, "r") as fp:
            return fp.read(int(maxsize))
    except IOError:
        return 'Log file %s is not readable' % logfile


if fact['os']['name'] == 'windows':
    class WindowsSystemAPI(SystemAPI):
        _LOG_FILE = os.path.join(__node__['log_dir'], 'scalarizr.log')
        _DEBUG_LOG_FILE = os.path.join(__node__['log_dir'], 'scalarizr_debug.log')
        _UPDATE_LOG_FILE = os.path.join(__node__['log_dir'], 'scalarizr_update.log')
        _pending_hostname = None

        @rpc.command_method
        def set_hostname(self, hostname=None):
            super(WindowsSystemAPI, self).set_hostname(hostname)
            self._pending_hostname = hostname

        @rpc.query_method
        def get_hostname(self):
            if self._pending_hostname:
                return self._pending_hostname
            return super(WindowsSystemAPI, self).get_hostname()

        @coinitialized
        @rpc.query_method
        def dist(self):
            uname = platform.uname()
            return dict(system=uname[0], release=uname[2], version=uname[3])

        @coinitialized
        @rpc.query_method
        def uptime(self):
            # pylint: disable=W0603
            wmi = client.GetObject('winmgmts:')
            win_os = next(iter(wmi.InstancesOf('Win32_OperatingSystem')))
            local_time, tz_op, tz_hh60mm = re.split(r'(\+|\-)', win_os.LastBootUpTime)
            local_time = local_time.split('.')[0]
            local_time = time.mktime(time.strptime(local_time, '%Y%m%d%H%M%S'))
            tz_seconds = int(tz_hh60mm) * 60
            if tz_op == '+':
                return time.time() - local_time + tz_seconds
            else:
                return time.time() - local_time - tz_seconds

        @rpc.query_method
        def uname(self):
            uname = platform.uname()
            return dict(zip(
                ('system', 'node', 'release', 'version', 'machine', 'processor'), uname
            ))

        @coinitialized
        @rpc.query_method
        def statvfs(self, mpoints=None):
            wmi = client.GetObject('winmgmts:')

            # mpoints == disks letters on Windows
            mpoints = map(lambda s: s[0].lower(), mpoints)
            if not isinstance(mpoints, list):
                raise Exception('Argument "mpoints" should be a list of strings, '
                            'not %s' % type(mpoints))
            ret = {}
            for disk in wmi.InstancesOf('Win32_LogicalDisk'):
                letter = disk.DeviceId[0].lower()
                if letter in mpoints:
                    ret[letter] = self._format_statvfs(disk)
            return ret


        @coinitialized
        @rpc.query_method
        def mounts(self):
            wmi = client.GetObject('winmgmts:')

            ret = {}
            for disk in wmi.InstancesOf('Win32_LogicalDisk'):
                letter = disk.DeviceId[0].lower()
                entry = {
                    'device': letter,
                    'mpoint': letter
                }
                entry.update(self._format_statvfs(disk))
                ret[letter] = entry
            return ret

        def _format_statvfs(self, disk):
            return {
                'total': int(disk.Size) / 1024 if disk.Size else None,  # Kb
                'free': int(disk.FreeSpace) / 1024 if disk.FreeSpace else None  # Kb
            }


    SystemAPI = WindowsSystemAPI
