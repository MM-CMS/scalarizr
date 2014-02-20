import os
import socket
import glob
import logging

from scalarizr import rpc
from scalarizr.node import __node__
from scalarizr import linux
from scalarizr.linux import pkgmgr
from scalarizr.util import Singleton, software
from scalarizr import exceptions
from scalarizr.util import initdv2
from scalarizr.util import firstmatched

LOG = logging.getLogger(__name__)


__tomcat__ = __node__['tomcat']
__tomcat__.update({
    'catalina_home_dir': None,
    'java_home': firstmatched(lambda path: os.access(path, os.X_OK), [
            linux.system('echo $JAVA_HOME', shell=True)[0].strip(),
            '/usr/java/default'], 
            '/usr'),
    'config_dir': None,
    'install_type': None
})


def augload():
    path = __tomcat__['config_dir']
    return [
        'set /augeas/load/Xml/incl[last()+1] "{0}/*.xml"'.format(path),
        'load',
        'defvar service /files{0}/server.xml/Server/Service'.format(path)                       
    ]


def augtool(script_lines):
    augscript = augload() + script_lines
    augscript = '\n'.join(augscript)
    LOG.debug('augscript: %s', augscript)
    return linux.system(('augtool', ), stdin=augscript)[0].strip()


class CatalinaInitScript(initdv2.ParametrizedInitScript):
    def __init__(self):
        initdv2.ParametrizedInitScript.__init__(self, 'tomcat', 
                __tomcat__['catalina_home_dir'] + '/bin/catalina.sh')
        self.server_port = None

    def status(self):
        if not self.server_port:
            out = augtool(['print /files{0}/server.xml/Server/#attribute/port'.format(__tomcat__['config_dir'])])
            self.server_port = out.split(' = ')[-1]
            self.server_port = int(self.server_port.strip('"'))

        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            sock.connect(('', self.server_port))
            return initdv2.Status.RUNNING
        except socket.error:
            return initdv2.Status.NOT_RUNNING
        finally:
            try:
                sock.close()
            except:
                pass


class TomcatAPI(object):

    __metaclass__ = Singleton
    last_check = False

    def _find_service(self):
        # try to read CATALINA_HOME from environment
        __tomcat__['catalina_home_dir'] = linux.system('echo $CATALINA_HOME', shell=True)[0].strip()
        if not __tomcat__['catalina_home_dir']:
            # try to locate CATALINA_HOME in /opt/apache-tomcat*
            try:
                __tomcat__['catalina_home_dir'] = glob.glob('/opt/apache-tomcat*')[0]
            except IndexError:
                pass

        if __tomcat__['catalina_home_dir']:
            __tomcat__['install_type'] = 'binary'
            __tomcat__['config_dir'] = '{0}/conf'.format(__tomcat__['catalina_home_dir'])
            init_script_path = '/etc/init.d/tomcat'
            if os.path.exists(init_script_path):
                return initdv2.ParametrizedInitScript('tomcat', init_script_path)
            else:
                return CatalinaInitScript()
        else:
            __tomcat__['install_type'] = 'package'
            if linux.os.debian_family:
                if (linux.os['name'] == 'Ubuntu' and linux.os['version'] >= (12, 4)) or \
                    (linux.os['name'] == 'Debian' and linux.os['version'] >= (7, 0)):
                    tomcat_version = 7
                else:
                    tomcat_version = 6
            else:
                tomcat_version = 6
            __tomcat__['config_dir'] = '/etc/tomcat{0}'.format(tomcat_version)
            init_script_path = '/etc/init.d/tomcat{0}'.format(tomcat_version)  
            return initdv2.ParametrizedInitScript('tomcat', init_script_path)

    def __init__(self):
        self.service = self._find_service()

    @rpc.command_method
    def start_service(self):
        self.service.start()

    @rpc.command_method
    def stop_service(self):
        self.service.stop()

    @rpc.command_method
    def reload_service(self):
        self.service.reload()

    @rpc.command_method
    def restart_service(self):
        self.service.restart()

    @rpc.command_method
    def get_service_status(self):
        return self.service.status()

    @classmethod
    def check_software(cls, installed_packages=None):
        try:
            TomcatAPI.last_check = False
            os_name = linux.os['name'].lower()
            os_vers = linux.os['version']
            if os_name == 'ubuntu':
                if os_vers >= '12':
                    pkgmgr.check_dependency(['tomcat7', 'tomcat7-admin'], installed_packages)
                elif os_vers >= '10':
                    pkgmgr.check_dependency(['tomcat6', 'tomcat6-admin'], installed_packages)
            elif os_name == 'debian':
                if os_vers >= '7':
                    pkgmgr.check_dependency(['tomcat7', 'tomcat7-admin'], installed_packages)
                elif os_vers >= '6':
                    pkgmgr.check_dependency(['tomcat6', 'tomcat6-admin'], installed_packages)
            elif linux.os.redhat_family or linux.os.oracle_family:
                pkgmgr.check_dependency(['tomcat6', 'tomcat6-admin-webapps'], installed_packages)
            else:
                raise exceptions.UnsupportedBehavior('tomcat',
                    "'tomcat' behavior is only supported on " +\
                    "Debian, RedHat and Oracle operating system family"
                )
            TomcatAPI.last_check = True
        except pkgmgr.DependencyError as e:
            software.handle_dependency_error(e, 'tomcat')
