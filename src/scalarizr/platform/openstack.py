import os
import re
import sys
import json
import logging
import urllib2
import contextlib

import mock
import novaclient
import swiftclient
import cinderclient.exceptions
import novaclient.exceptions
from cinderclient.v2 import client as cinder_client
from keystoneclient.auth.identity import v3 as auth_v3
from keystoneclient import session

# import novaclient (installing VC2010 on demand)
try:
    # pylint: disable=unused-import
    import netifaces
except ImportError as e:
    if 'DLL load failed' in str(e) and sys.platform == 'win32':
        # SCALARIZR-2115: Install Visual C++ 2010 redistributable to fix 'netifaces' import
        from scalarizr.util.wintool import install_vc2010_redist
        install_vc2010_redist()
from novaclient import client as nova_client


from rackspace_auth_openstack.plugin import RackspaceAuthPlugin

from scalarizr import node
from scalarizr import platform
from scalarizr.bus import bus
from scalarizr import linux
from scalarizr.util import LocalPool
from scalarizr.platform import PlatformError
from scalarizr.platform import NoCredentialsError, InvalidCredentialsError
from scalarizr.storage.transfer import Transfer, TransferProvider
from scalarizr.storage2.cloudfs import swift as swiftcloudfs


LOG = logging.getLogger(__name__)


def _create_keystone_v3_session():
    pl = node.__node__['platform']
    keystone_url = pl.get_access_data('keystone_url')

    if 'v3' not in keystone_url.split('/')[-1]:
        LOG.debug('Keystone is not v3')
        return None

    try:
        domain_name = pl.get_access_data('domain_name')
    except PlatformError:
        raise InvalidCredentialsError('No domain_name provided for keystone v3 auth')

    auth = auth_v3.Password(
        username=pl.get_access_data('username'),
        password=pl.get_access_data('password'),
        project_name=pl.get_access_data('tenant_name'),
        auth_url=keystone_url,
        user_domain_name=domain_name,
        project_domain_name=domain_name)
    return session.Session(auth=auth)


def _get_client_kwds(service_type):
    pl = node.__node__['platform']
    kwds = {
        'region_name': pl.get_access_data('cloud_location'),
        'service_type': service_type}

    v3_session = _create_keystone_v3_session()
    auth_url = pl.get_access_data('keystone_url')

    if v3_session:
        kwds['session'] = v3_session
    else:
        kwds['username'] = pl.get_access_data('username')
        kwds['api_key'] = pl.get_access_data('api_key') \
            or pl.get_access_data('password')
        kwds['project_id'] = pl.get_access_data('tenant_name')
        kwds['auth_url'] = auth_url

    if 'rackspacecloud' in auth_url:
        kwds['auth_plugin'] = RackspaceAuthPlugin()
        kwds['auth_system'] = 'rackspace'

    if not bool(pl.get_access_data('ssl_verify_peer')):
        kwds['insecure'] = True

    return kwds


def _create_nova_connection():
    try:
        kwds = _get_client_kwds('compute')
        import novaclient  # NameError: name 'novaclient' is not defined
        if hasattr(novaclient, '__version__') and os.environ.get('OS_AUTH_SYSTEM'):
            try:
                import novaclient.auth_plugin
                auth_plugin = novaclient.auth_plugin.load_plugin(os.environ['OS_AUTH_SYSTEM'])
                kwds['auth_plugin'] = auth_plugin
            except ImportError:
                pass
        conn = nova_client.Client('2', **kwds)
    except PlatformError:
        raise NoCredentialsError(sys.exc_info()[1])
    return conn


def _create_cinder_connection():
    try:
        conn = cinder_client.Client(**_get_client_kwds('volume'))
    except PlatformError:
        raise NoCredentialsError(sys.exc_info()[1])
    return conn


def _create_swift_connection():
    try:
        platform = node.__node__['platform']
        api_key = platform.get_access_data("api_key")
        password = platform.get_access_data("password")
        auth_url = platform.get_access_data("keystone_url")
        kwds = {}
        if 'rackspacecloud' in auth_url:
            auth_url = re.sub(r'v2\.\d$', 'v1.0', auth_url)
            kwds['auth_version'] = '1'
        else:
            kwds['auth_version'] = '3' if 'v3' in auth_url.split('/')[-1] else '2'
            kwds['tenant_name'] = platform.get_access_data("tenant_name")
        if not bool(platform.get_access_data('ssl_verify_peer')):
            kwds['insecure'] = True
        conn = swiftclient.Connection(
            authurl=auth_url,
            user=platform.get_access_data('username'),
            key=password or api_key,
            **kwds
        )
    except PlatformError:
        raise NoCredentialsError(sys.exc_info()[1])
    return conn


@contextlib.contextmanager
def use_proxy(proxy_cfg=None):
    """Mocks requests.utils.os.environ dictionary in the module <module_path>,
    so `requests` library in that module uses proxy <proxy_url>
    """
    if proxy_cfg is not None:
        proxy_proto = {0: 'http', 4: 'socks4', 5: 'socks5'}.get(proxy_cfg['type']) or 'http'

        auth = ':'.join(filter(None, (proxy_cfg.get('user'), proxy_cfg.get('pass'))))
        address = ':'.join(filter(None, (proxy_cfg.get('host'),
                                         proxy_cfg.get('port') and str(proxy_cfg['port']))))
        netloc = '@'.join(filter(None, (auth, address)))
        proxy_url = "{}://{}".format(proxy_proto, netloc)

        patch_env = dict(HTTP_PROXY=proxy_url, HTTPS_PROXY=proxy_url)
        proxy_patcher = mock.patch.dict('os.environ', patch_env)

        with proxy_patcher:
            yield
    else:
        yield


class NovaConnectionProxy(platform.ConnectionProxy):

    def invoke(self, *args, **kwds):
        with use_proxy(node.__node__['access_data'].get('proxy')):
            try:
                return super(NovaConnectionProxy, self).invoke(*args, **kwds)
            except (novaclient.exceptions.Unauthorized, novaclient.exceptions.Forbidden), e:
                raise InvalidCredentialsError(e)


class CinderConnectionProxy(platform.ConnectionProxy):

    def invoke(self, *args, **kwds):
        with use_proxy(node.__node__['access_data'].get('proxy')):
            try:
                return super(CinderConnectionProxy, self).invoke(*args, **kwds)
            except (cinderclient.exceptions.Unauthorized, cinderclient.exceptions.Forbidden), e:
                raise InvalidCredentialsError(e)


class SwiftConnectionProxy(platform.ConnectionProxy):

    def invoke(self, *args, **kwds):
        with use_proxy(node.__node__['access_data'].get('proxy')):
            try:
                return super(SwiftConnectionProxy, self).invoke(*args, **kwds)
            except:
                e = sys.exc_info()[1]
                if isinstance(e, swiftclient.ClientException) and (
                        re.search(r'.*Unauthorised.*', e.msg) or \
                        re.search(r'.*Authorization Failure.*', e.msg)):
                    raise InvalidCredentialsError(e)
                else:
                    raise


class OpenstackPlatform(platform.Platform):

    _meta_url = "http://169.254.169.254/openstack/latest/meta_data.json"
    _metadata = {}
    _userdata = None

    features = ['volumes', 'snapshots']
    name = 'openstack'

    def __init__(self):
        platform.Platform.__init__(self)
        if not linux.os.windows_family:
            # Work over [Errno -3] Temporary failure in name resolution
            # http://bugs.centos.org/view.php?id=4814
            os.chmod('/etc/resolv.conf', 0755)
        self._nova_conn_pool = LocalPool(_create_nova_connection)
        self._swift_conn_pool = LocalPool(_create_swift_connection)
        self._cinder_conn_pool = LocalPool(_create_cinder_connection)


    def _get_property(self, name):
        if not name in self._userdata:
            self.get_user_data()
        return self._userdata[name]

    def get_server_id(self):
        if node.__node__['farm_role_id']:
            global_variables = bus.queryenv_service.list_global_variables()
            return global_variables['public']['SCALR_CLOUD_SERVER_ID']
        else:
            nova = self.get_nova_conn()
            servers = nova.servers.list()
            my_ip = self.get_private_ip()
            for server in servers:
                ips = []
                ip_addr = 'private' in server.addresses and server.addresses['private'][0]['addr']
                if ip_addr:
                    ips.append(ip_addr)
                else:
                    ips = [address['addr']
                                for network in server.addresses.values()
                                for address in network]
                if my_ip in ips:
                    return server.id
            raise BaseException("Can't get server_id because we can't get "
                                "server private ip")

    def get_avail_zone(self):
        return self._get_property('availability_zone')

    def get_ssh_pub_key(self):
        return self._get_property('public_keys')  # TODO: take one key

    def _fetch_metadata(self):
        """
        Fetches whole metadata dict. Unlike Ec2LikePlatform,
        which fetches data for concrete key.
        """

        try:
            try:
                self._logger.debug('fetching meta-data from %s', self._meta_url)
                r = urllib2.urlopen(self._meta_url)
                response = r.read().strip()
                meta = json.loads(response)
            except:
                self._logger.debug('failed to fetch meta-data: %s', sys.exc_info()[1])
            else:
                if meta.get('meta'):
                    return meta
                else:
                    self._logger.debug('meta-data fetched, but has empty user-data (a "meta" key),'
                                       ' try next method')

            return {'meta': self._fetch_metadata_from_file()}
        except:
            raise platform.PlatformError, 'failed to fetch meta-data', sys.exc_info()[2]

    def _fetch_metadata_from_file(self):
        self._logger.debug('fetching meta-data from files')
        if self._userdata is None:
            private_dir_ud_path = os.path.join(node.__node__['private_dir'], '.user-data')
            for path in ('/etc/.scalr-user-data', private_dir_ud_path):
                if os.path.exists(path):
                    self._logger.debug('using file %s', path)
                    rawmeta = None
                    with open(path, 'r') as fp:
                        rawmeta = fp.read()
                    if not rawmeta:
                        raise platform.PlatformError("Empty user-data")
                    self._logger.info('Use user-data from %s', path)
                    return self._parse_user_data(rawmeta)
        return self._userdata

    def set_access_data(self, access_data):
        platform.Platform.set_access_data(self, access_data)
        # if it's Rackspace NG, we need to set env var CINDER_RAX_AUTH
        # and NOVA_RAX_AUTH for proper nova and cinder authentication
        if 'rackspacecloud' in access_data["keystone_url"]:
            # python-novaclient has only configuration with environ variables
            # to enable Rackspace specific authentification
            os.environ["CINDER_RAX_AUTH"] = "True"
            os.environ["NOVA_RAX_AUTH"] = "True"
            os.environ["OS_AUTH_SYSTEM"] = "rackspace"


    def get_nova_conn(self):
        return NovaConnectionProxy(self._nova_conn_pool)

    def get_cinder_conn(self):
        return CinderConnectionProxy(self._cinder_conn_pool)

    def get_swift_conn(self):
        return SwiftConnectionProxy(self._swift_conn_pool)


def get_platform():
    # Filter keystoneclient* and swiftclient* log messages
    class FalseFilter(object):
        def filter(self, record):
            return False
    for cat in ('keystoneclient', 'swiftclient'):
        log = logging.getLogger(cat)
        log.addFilter(FalseFilter())

    return OpenstackPlatform()


class SwiftTransferProvider(TransferProvider):
    schema = 'swift'

    _logger = None

    def __init__(self):
        self._logger = logging.getLogger(__name__)
        self._driver = swiftcloudfs.SwiftFileSystem()
        TransferProvider.__init__(self)

    def put(self, local_path, remote_path):
        self._logger.info('Uploading %s to Swift under %s', local_path, remote_path)
        return self._driver.put(local_path, os.path.join(remote_path, os.path.basename(local_path)))

    def get(self, remote_path, local_path):
        self._logger.info('Downloading %s from Swift to %s', remote_path, local_path)
        return self._driver.get(remote_path, local_path)


    def list(self, remote_path):
        return self._driver.ls(remote_path)


Transfer.explore_provider(SwiftTransferProvider)


# Logging

class OpenStackCredentialsLoggerFilter(object):

    request_re = re.compile('(X-Auth[^:]+:)([^\'"])+')
    response_re = re.compile('(.*)({["\']access.+})(.*)')

    def filter(self, record):
        message = record.getMessage()
        record.args = ()

        if "passwordCredentials" in message:
            record.msg = 'Requested authentication, credentials are hidden'
            return True

        search_res = re.search(self.response_re, message)
        if search_res:
            try:
                response_part_str = search_res.group(2)
                response = json.loads(response_part_str)
                response['access']['token'] = '<HIDDEN>'
                response['access']['user'] = '<HIDDEN>'
                altered_resp = json.dumps(response)
                record.msg = search_res.group(1) + altered_resp + search_res.group(3)
                return True
            except:
                return False

        if "X-Auth" in message:
            record.msg = re.sub(self.request_re, r'\1 <HIDDEN>', message)
            return True


openstack_filter = OpenStackCredentialsLoggerFilter()
for logger_name in ('keystoneclient.client', 'novaclient.client', 'cinderclient.client'):
    logger = logging.getLogger(logger_name)
    logger.addFilter(openstack_filter)
