# coding: utf-8

"""
Created on 01.14.2015
@author: Eugeny Kurkovich
"""

from behave import *
import mock
import shutil
import os
import sys
try:
    import win32file
except ImportError:
    pass

import scalarizr.util
scalarizr.util.get_metadata = mock.Mock()
from scalarizr.updclient.app import UpdClient

from scalarizr.updclient import pkgmgr
storage_path = 'c:\\tmp' if sys.platform == 'win32' else '/tmp'
mock.patch('scalarizr.updclient.pkgmgr.STORAGE_DIR', storage_path).start()

import scalarizr.linux
from scalarizr.util import system2


def before_all(context):
    context.platform = scalarizr.linux.os['family']

def before_scenario(context, scenario):
    if context.platform not in scenario.name:
        scenario.skip()


def before_feature(context, feature):
    if u'pkgmgr' in feature.tags:
        if context.platform == "RedHat":
            context.pkgmgr = pkgmgr.YumManager
        elif context.platform == 'Windows':
            context.pkgmgr = pkgmgr.WinPackageManager
            context.pkgmgr_exc_class = pkgmgr.WinPackageManagerError
        else:
            context.pkgmgr = pkgmgr.AptManager

    if u'updclient' in feature.tags:
        client = UpdClient()

        client.api.client_mode = 'solo'
        client.api.get_system_id = mock.Mock(return_value='123')
        client.api._init_queryenv = mock.Mock()
        client.api.queryenv = mock.Mock()
        client.api.queryenv.list_farm_role_params = mock.Mock(return_value={})
        client.api.queryenv.get_global_config = mock.Mock(return_value={
            'params': {'scalr.id': None, 'scalr.version': None}})
        client.api._ensure_daemon = mock.Mock()
        client.api.store = mock.Mock()
        client.api.status_file = ''
        #client.api._sync = mock.Mock()
        client.api.daemon = mock.Mock()
        client.api.scalarizr = mock.Mock()
        client.api.meta.user_data = mock.Mock(return_value={
            'platform': 'ec2',
            'serverid': '123',
            'p2p_producer_endpoint': 'https://my.scalr.com/messaging',
            'queryenv_url': 'https://my.scalr.com/query-env',
            'farm_roleid': '312'})
        client.api._init_services = mock.Mock()
        client.api.uninstall = mock.Mock()

        if scalarizr.linux.os.windows:
            mock.patch('scalarizr.updclient.api.UpdClientAPI.package',
                new_callable=mock.PropertyMock(return_value='mock-scalarizr')).start()
            deps_callable = mock.MagicMock(return_value=lambda self, ver:\
                None)
            mock.patch('scalarizr.updclient.api.UpdClientAPI.deps',
                new_callable=deps_callable).start()
        else:
            mock.patch('scalarizr.updclient.api.UpdClientAPI.package',
                new_callable=mock.PropertyMock(return_value='mock-scalarizr-ec2')).start()
            deps_callable = mock.MagicMock(return_value=lambda self, ver:\
                [{'name': 'mock-scalarizr', 'version': ver}])
            mock.patch('scalarizr.updclient.api.UpdClientAPI.deps',
                new_callable=deps_callable).start()

        context.client = client


def after_feature(context, feature):
    if context.platform == 'RedHat':
        scalarizr.linux.system(('rpm',
            '-e',
            'mock-scalarizr',
            'mock-scalarizr-ec2'),
            raise_exc=False)
    elif context.platform == 'Windows':
        cleanup_script_path = 'C:\\tmp\\cleanup.ps1'
        with open(cleanup_script_path, 'w') as fp:
            cleanup_script = '$app = Get-WmiObject -Class Win32_Product -Filter ' \
                '"Name LIKE \'Mock-Scal%\'"\nforeach($x in $app) {$x.Uninstall()}'
            fp.write(cleanup_script)
        system2(('C:\\WINDOWS\\system32\\WindowsPowerShell\\v1.0\\powershell.exe',
            '%s' % cleanup_script_path),
            shell=True)
        os.remove('%s' % cleanup_script_path)
        system2(("rmdir", "/Q", "/S", "C:\\opt\\mock-scalarizr"), shell=True)
        system2(("rmdir", "/Q", "/S", "C:\\tmp\\mock-scalarizr"), shell=True)
        system2(("rmdir", "/Q", "/S", "C:\\tmp\\.index"), shell=True)
    else:
        scalarizr.linux.system(('dpkg',
            '--purge',
            'mock-scalarizr',
            'mock-scalarizr-ec2'),
            raise_exc=False)

    if os.path.exists('/tmp/mock-scalarizr-ec2'):
        shutil.rmtree('/tmp/mock-scalarizr-ec2')

    if os.path.exists('/tmp/mock-scalarizr'):
        shutil.rmtree('/tmp/mock-scalarizr')
