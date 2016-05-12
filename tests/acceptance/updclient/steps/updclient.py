# -*- coding: utf-8 -*-
from behave import *

import os
import mock

from scalarizr import linux

if linux.os['family'] == 'RedHat':
    def check_version(pkg):
        out, err, returncode = linux.system(('rpm', '-q', '--queryformat', '%{VERSION}', pkg), raise_exc=False)
        version = '' if returncode else out
        return version
elif linux.os['family'] == 'Debian':
    def check_version(pkg):
        out, err, returncode = linux.system(('dpkg-query', '-W', '-f=${Version} ${Status}', pkg), raise_exc=False)
        if returncode == 0:
            version, status = out.split(' ', 1)
            if status == 'install ok installed':
                return version
        return ''
elif linux.os['family'] == 'Windows':
    def check_version(pkg):
        install_dir = "C:\\opt\\"
        manifest_path = os.path.join(install_dir, pkg, 'current', 'version-manifest.txt')
        omnibus_packages = None
        with open(manifest_path, 'r') as fp:
            omnibus_packages = fp.readlines()
        for l in omnibus_packages:
            pkg_info = l.split()
            if pkg_info[0] == pkg:
                return pkg_info[1]
        return ''


@given('I have a scalarizr repository "{repo}" with the latest version "{version}"')
def step(context, repo, version):
    context.repo = repo
    context.version = version

@given('I have a scalarizr repository "{repo}" with a broken version of scalarizr')
def step(context, repo):
    context.repo = repo

@when('I bootstrap updclient')
def step(context):
    with mock.patch('scalarizr.updclient.api.value_for_repository', return_value=context.repo):
        context.client.api.bootstrap()

@when('i call update')
def step(context):
    with mock.patch('scalarizr.updclient.api.value_for_repository', return_value=context.repo):
        with mock.patch('scalarizr.api.operation.__node__'):
            context.client.api.update(force=True)

@then('it installs the latest version of scalarizr from the given repository')
def step(context):
    version = check_version('mock-scalarizr')
    assert version == context.version

@then('it tries to install the broken version and rollbacks to the previous vesion "{version}"')
def step(context, version):
        assert check_version('mock-scalarizr') == version
