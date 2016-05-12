import os
import ConfigParser
import sys
import copy
import json
import logging

from scalarizr import linux


LOG = logging.getLogger(__name__)


class Store(object):

    def __repr__(self):
        return '<%s at %s>' % (type(self).__name__, hex(id(self)))

    def __getitem__(self, key):
        raise NotImplementedError()

    def __setitem__(self, key, name):
        raise NotImplementedError()


class Compound(dict):
    '''
    pyline: disable=E1101
    '''
    def __init__(self, patterns=None):
        super(Compound, self).__init__()
        patterns = patterns or {}
        for pattern, store in list(patterns.items()):
            keys = pattern.split(',')
            for key in keys:
                super(Compound, self).__setitem__(key, store)

    def __getattr__(self, name):
        try:
            return self.__getitem__(name)
        except KeyError:
            raise AttributeError(name)

    def __setitem__(self, key, value):
        keys = key.split(',')
        if len(keys) > 1:
            for key in keys:
                self[key] = value
            return
        try:
            value_now = dict.__getitem__(self, key)
        except KeyError:
            value_now = None
        if isinstance(value_now, Store) and not isinstance(value, Store):
            value_now.__setitem__(key, value)
        else:
            super(Compound, self).__setitem__(key, value)

    def __getitem__(self, key):
        value = dict.__getitem__(self, key)
        if isinstance(value, Store):
            return value.__getitem__(key)
        else:
            return value

    def copy(self):
        ret = Compound()
        for key in self:
            value = dict.__getitem__(self, key)
            if isinstance(value, Store):
                value = copy.deepcopy(value)
            ret[key] = value
        return ret

    def update(self, values):
        for key, value in list(values.items()):
            self[key] = value

    def __repr__(self):
        ret = {}
        for key in self:
            value = dict.__getitem__(self, key)
            if isinstance(value, Store):
                value = repr(value)
            ret[key] = value
        return repr(ret)


class Json(Store):

    def __init__(self, filename, fn):
        '''
        Example:
        jstore = Json('/etc/scalr/private.d/storage/mysql.json',
            'scalarizr.storage2.volume')
        '''
        self.filename = filename
        self.fn = fn
        self._obj = None

    def __getitem__(self, key):
        if not self._obj:
            try:
                with open(self.filename, 'r') as fp:
                    kwds = json.load(fp)
            except:
                raise KeyError('{}: {}'.format(key, sys.exc_info()[1]))
            else:
                if isinstance(self.fn, basestring):
                    self.fn = _import(self.fn)
                self._obj = self.fn(**kwds)
        return self._obj

    def __setitem__(self, key, value):
        self._obj = value
        if hasattr(value, 'config'):
            value = value.config()
        dirname = os.path.dirname(self.filename)
        if not os.path.exists(dirname):
            os.makedirs(dirname)
        with open(self.filename, 'w+') as fp:
            json.dump(value, fp)


class Ini(Store):
    def __init__(self, filenames, section, mapping=None):
        if not hasattr(filenames, '__iter__') or isinstance(filenames, basestring):
            filenames = [filenames]
        self.filenames = filenames
        self.saved_mtimes = [0 for _ in range(0, len(filenames))]
        self.section = section
        self.ini = ConfigParser.ConfigParser() 
        self.mapping = mapping or {}

    def _reload(self):
        mtimes = [os.path.exists(filename) and os.stat(filename).st_mtime or 0 \
                for filename in self.filenames]
        # LOG.debug('mtimes: {} saved_mtimes: {}'.format(mtimes, self.saved_mtimes))
        if any(mtimes[i] > self.saved_mtimes[i] \
                for i in range(0, len(self.filenames))):
            self.ini = ConfigParser.ConfigParser() 
            for i, filename in enumerate(self.filenames):
                if os.path.exists(filename):
                    LOG.debug('Reloading {}'.format(filename))
                    self.ini.read(filename)
                self.saved_mtimes[i] = mtimes[i]


    def __getitem__(self, key):
        self._reload()
        if not self.ini:
            raise KeyError(key)
        if key in self.mapping:
            key = self.mapping[key]
        try:
            val = self.ini.get(self.section, key)
            # LOG.debug('Get {}.{} is {}'.format(self.section, key, val))
            return val
        except ConfigParser.Error as e:
            raise KeyError, '{}: {}'.format(key, e), sys.exc_info()[2]

    def __setitem__(self, key, value):
        self._reload()
        if value is None:
            value = ''
        elif isinstance(value, bool):
            value = str(int(value))
        else:
            value = str(value)

        filename = self.filenames[-1]
        ini = ConfigParser.ConfigParser()
        if os.path.exists(filename):
            ini.read(filename)

        if not ini.has_section(self.section):
            ini.add_section(self.section)
        if not self.ini.has_section(self.section):
            self.ini.add_section(self.section)

        if key in self.mapping:
            key = self.mapping[key]
        # LOG.debug('Set {}.{}={} in {}'.format(
        #        self.section, key, value, filename))
        ini.set(self.section, key, value)
        self.ini.set(self.section, key, value)
        with open(filename, 'w+') as fp:
            ini.write(fp)



class RedisIni(Ini):

    def __getitem__(self, key):
        try:
            value = super(RedisIni, self).__getitem__(key)
            if key in ('use_password', 'replication_master',):
                if value in (None, ''):
                    value = True
                else:
                    value = bool(int(value))
        except KeyError:
            if 'persistence_type' == key:
                value = 'snapshotting'
                self.__setitem__(key, value)
            elif 'master_password' == key:
                value = None
            else:
                raise
        return value


class IniOption(Ini):
    def __init__(self, filenames, section, option,
                    getfilter=None, setfilter=None):
        self.option = option
        self.getfilter = getfilter
        self.setfilter = setfilter
        super(IniOption, self).__init__(filenames, section)

    def __getitem__(self, key):
        value = super(IniOption, self).__getitem__(self.option)
        if self.getfilter:
            return self.getfilter(value)
        return value

    def __setitem__(self, key, value):
        if self.setfilter:
            value = self.setfilter(value)
        super(IniOption, self).__setitem__(self.option, value)


class File(Store):
    def __init__(self, filename):
        self.filename = filename

    def __getitem__(self, key):
        try:
            with open(self.filename) as fp:
                return fp.read().strip()
        except IOError as e:
            raise KeyError('{}: {}'.format(key, e))

    def __setitem__(self, key, value):
        with open(self.filename, 'w+') as fp:
            fp.write(str(value).strip())


class BoolFile(Store):
    def __init__(self, filename):
        self.filename = filename

    def __getitem__(self, key):
        return os.path.isfile(self.filename)

    def __setitem__(self, key, value):
        if value:
            open(self.filename, 'w+').close()
        else:
            if os.path.isfile(self.filename):
                os.remove(self.filename)


class StateFile(File):
    def __getitem__(self, key):
        try:
            return super(StateFile, self).__getitem__(key)
        except KeyError:
            return 'unknown'


class State(Store):
    def __init__(self, key):
        self.key = key

    def __getitem__(self, key):
        from scalarizr.config import STATE
        return STATE[self.key]

    def __setitem__(self, key, value):
        from scalarizr.config import STATE
        STATE[self.key] = value


class Attr(Store):

    def __init__(self, module, attr):
        self.module = module
        self.attr = attr
        self.getter = None

    def __getitem__(self, key):
        try:
            if isinstance(self.module, basestring):
                self.module = _import(self.module)
            if not self.getter:
                def getter():
                    path = self.attr.split('.')
                    base = self.module
                    for name in path[:-1]:
                        base = getattr(base, name)
                    return getattr(base, path[-1])
                self.getter = getter
        except:
            raise KeyError('{}: {}'.format(key, sys.exc_info()[1]))
        return self.getter()


class Call(Attr):

    def __getitem__(self, key):
        attr = Attr.__getitem__(self, key)
        return attr()


def _import(objectstr):
    try:
        __import__(objectstr)
        return sys.modules[objectstr]
    except ImportError:
        module_s, _, attr = objectstr.rpartition('.')
        __import__(module_s)
        try:
            return getattr(sys.modules[module_s], attr)
        except (KeyError, AttributeError):
            raise ImportError('No module named %s' % attr)


# FOR FUTURE USE
# class DelayedCall(Store):

#     def __init__(self, function, cache=False):
#         self._function = function
#         self._was_called = False
#         self._cache = cache
#         self._last_value = None

#     def __getitem__(self, key):
#         if self._cache and self._was_called:
#             return self._last_value
#         self._was_called = True
#         self._last_value = self._function()
#         return self._last_value


__node__ = Compound()


node = {}


if linux.os.windows_family:
    node['install_dir'] = 'C:\\opt\\scalarizr\\current'
    self_dir = os.path.normpath(os.path.join(node['install_dir'], '..\\'))
    node['etc_dir'] = os.path.join(self_dir, 'etc')
    node['log_dir'] = os.path.join(self_dir, 'var\\log')
    node['run_dir'] = os.path.join(self_dir, 'var\\run')
    node['updclient'] = {'cache_dir': os.path.join(self_dir, 'var\\cache\\scalr\\updclient\\pkgmgr')}
else:
    node['install_dir'] = '/opt/scalarizr'
    node['etc_dir'] = '/etc/scalr'
    node['log_dir'] = '/var/log'
    node['run_dir'] = '/var/run'
    node['updclient'] = {'cache_dir': "/var/cache/scalr/updclient/pkgmgr"}


def reload_params(default=False):
    if default:
        __node__.update(node)

    private_dir = __node__['etc_dir'] + '/private.d'
    public_dir = __node__['etc_dir'] + '/public.d'
    __node__['public_dir'] = public_dir
    __node__['private_dir'] = private_dir
    __node__['storage_dir'] = __node__['etc_dir'] + '/storage'
    __node__['scripts_dir'] = os.path.join(__node__['install_dir'], 'scripts')

    __node__['global_timeout'] = 2400

    __node__.update({
        'server_id,role_id,farm_id,farm_role_id,env_id,role_name,server_index,'
        'queryenv_url,cloud_storage_path':
            Ini(os.path.join(private_dir, 'config.ini'), 'general'),
        'message_format,producer_url':
            Ini(os.path.join(private_dir, 'config.ini'), 'messaging_p2p'),
        'platform_name,crypto_key_path': Ini(os.path.join(public_dir, 'config.ini'), 'general'),
        'platform': Attr('scalarizr.bus', 'bus.platform'),
        'public_ip': Call('scalarizr.bus', 'bus.platform.get_public_ip'),
        'private_ip': Call('scalarizr.bus', 'bus.platform.get_private_ip'),
        'behavior': IniOption([public_dir + '/config.ini', private_dir + '/config.ini'],
            'general', 'behaviour',
            lambda val: val.strip().split(','),
            ','.join),
        'running': False,
        'state': StateFile(private_dir + '/.state'),
        'rebooted': BoolFile(private_dir + '/.reboot'),
        'halted': BoolFile(private_dir + '/.halt'),
        'cloud_location' : IniOption(private_dir + '/config.ini', 'general', 'region'),
        'periodical_executor': Attr('scalarizr.bus', 'bus.periodical_executor')})

    __node__['embedded_bin_dir'] = os.path.join(__node__['install_dir'], 'embedded', 'bin')

    for d in (os.path.join(__node__['install_dir'], 'share'),
        '/usr/share/scalr',
        '/usr/local/share/scalr'):
        if os.access(d, os.F_OK):
            __node__['share_dir'] = d
            break
    else:
        __node__['share_dir'] = os.path.join(__node__['install_dir'], 'share')

    __node__['scalr'] = Compound({
        'version': File(private_dir + '/.scalr-version'),
        'id': Ini(private_dir + '/config.ini', 'general', {'id': 'scalr_id'})
    })

    class BaseSettings(dict):
        def __init__(self, *args, **kwds):
            super(BaseSettings, self).__init__(*args, **kwds)
            self.sanitize()

        def update(self, *args, **kwds):
            super(BaseSettings, self).update(*args, **kwds)
            self.sanitize()

        def sanitize(self):
            self['keep_scripting_logs_time'] = int(self.get('keep_scripting_logs_time', 86400))
            self['abort_init_on_script_fail'] = int(self.get('abort_init_on_script_fail', False))
            self['union_script_executor'] = int(self.get('union_script_executor', False))
            self['api_port'] = int(self.get('api_port', 8010))
            self['messaging_port'] = int(self.get('messaging_port', 8013))


    __node__['base'] = BaseSettings()
    __node__['base'].sanitize()
    __node__['access_data'] = {}

    for behavior in ('mysql', 'mysql2', 'percona', 'mariadb'):
        section = 'mysql2' if behavior in ('percona', 'mariadb') else behavior
        __node__[behavior] = Compound({
            'volume,volume_config': Json('%s/storage/%s.json' % (private_dir, 'mysql'),
                'scalarizr.storage2.volume'),
            'root_password,repl_password,stat_password,log_file,log_pos,replication_master':
                Ini('%s/%s.ini' % (private_dir, behavior), section),
            'mysqldump_options': Ini('%s/%s.ini' % (public_dir, behavior), section)
        })

    __node__['redis'] = Compound({
        'volume,volume_config': Json(
            '%s/storage/%s.json' % (private_dir, 'redis'), 'scalarizr.storage2.volume'),
        'replication_master,persistence_type,use_password,master_password': RedisIni(
                        '%s/%s.ini' % (private_dir, 'redis'), 'redis')
    })

    __node__['rabbitmq'] = Compound({
        'volume,volume_config': Json('%s/storage/%s.json' % (private_dir, 'rabbitmq'),
            'scalarizr.storage2.volume'),
        'password,node_type,cookie,hostname':
            Ini('%s/%s.ini' % (private_dir, 'rabbitmq'), 'rabbitmq')
    })

    __node__['postgresql'] = Compound({
        'volume,volume_config': Json('%s/storage/%s.json' % (private_dir, 'postgresql'),
            'scalarizr.storage2.volume'),
        'replication_master,pg_version,scalr_password,root_password, root_user': Ini(
            '%s/%s.ini' % (private_dir, 'postgresql'), 'postgresql')
    })

    __node__['mongodb'] = Compound({
        'volume,volume_config': Json('%s/storage/%s.json' % (private_dir, 'mongodb'),
            'scalarizr.storage2.volume'),
        'snapshot,shanpshot_config': Json('%s/storage/%s-snap.json' % (private_dir, 'mongodb'),
            'scalarizr.storage2.snapshot'),
        'shards_total,password,replica_set_index,shard_index,keyfile':
            Ini('%s/%s.ini' % (private_dir, 'mongodb'), 'mongodb')
    })

    __node__['nginx'] = Compound({
        'app_port,upstream_app_role': Ini('%s/%s.ini' % (public_dir, 'www'), 'www')
    })

    __node__['apache'] = Compound({
        'vhosts_path,apache_conf_path': Ini('%s/%s.ini' % (public_dir, 'app'), 'app')
    })

    __node__['tomcat'] = {}

    __node__['ec2'] = Compound({
        't1micro_detached_ebs': State('ec2.t1micro_detached_ebs'),
        'hostname_as_pubdns': Ini('%s/%s.ini' % (public_dir, 'ec2'), 'ec2'),
        'ami_id': Call('scalarizr.bus', 'bus.platform.get_ami_id'),
        'kernel_id': Call('scalarizr.bus', 'bus.platform.get_kernel_id'),
        'ramdisk_id': Call('scalarizr.bus', 'bus.platform.get_ramdisk_id'),
        'instance_id': Call('scalarizr.bus', 'bus.platform.get_instance_id'),
        'instance_type': Call('scalarizr.bus', 'bus.platform.get_instance_type'),
        'avail_zone': Call('scalarizr.bus', 'bus.platform.get_avail_zone'),
        'region': Call('scalarizr.bus', 'bus.platform.get_region'),
        'connect_ec2': Attr('scalarizr.bus', 'bus.platform.get_ec2_conn'),
        'connect_s3': Attr('scalarizr.bus', 'bus.platform.get_s3_conn')
    })
    __node__['cloudstack'] = Compound({
        'connect_cloudstack': Attr('scalarizr.bus', 'bus.platform.get_cloudstack_conn'),
        'instance_id': Call('scalarizr.bus', 'bus.platform.get_instance_id'),
        'zone_id': Call('scalarizr.bus', 'bus.platform.get_avail_zone_id'),
        'zone_name': Call('scalarizr.bus', 'bus.platform.get_avail_zone')
    })
    __node__['openstack'] = Compound({
        'connect_nova': Attr('scalarizr.bus', 'bus.platform.get_nova_conn'),
        'connect_cinder': Attr('scalarizr.bus', 'bus.platform.get_cinder_conn'),
        'connect_swift': Attr('scalarizr.bus', 'bus.platform.get_swift_conn'),
        'server_id': Call('scalarizr.bus', 'bus.platform.get_server_id')
    })

    __node__['gce'] = Compound({
        'connect_compute': Attr('scalarizr.bus', 'bus.platform.get_compute_conn'),
        'connect_storage': Attr('scalarizr.bus', 'bus.platform.get_storage_conn'),
        'project_id': Call('scalarizr.bus', 'bus.platform.get_project_id'),
        'instance_id': Call('scalarizr.bus', 'bus.platform.get_instance_id'),
        'zone': Call('scalarizr.bus', 'bus.platform.get_zone')
    })

    __node__['scalr'] = Compound({
        'version': File(private_dir + '/.scalr-version'),
        'id': Ini(private_dir + '/config.ini', 'general', {'id': 'scalr_id'})
    })

    __node__['messaging'] = Compound({
        'send': Attr('scalarizr.bus', 'bus.messaging_service.send')
    })

    __node__['access_data'] = {}
    __node__['bollard'] = None
    __node__['events'] = Attr('scalarizr.bus', 'bus')
    __node__['queryenv'] = Attr('scalarizr.bus', 'bus.queryenv_service')


reload_params(default=True)
