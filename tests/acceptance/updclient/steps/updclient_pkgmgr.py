# -*- coding: utf-8 -*-
"""
Created on 12.08.2014
@author: Eugeny Kurkovich
"""

import os
import logging
from behave import *
from scalarizr import linux
from distutils.version import LooseVersion
from datetime import datetime
from glob import glob
from mock import patch


use_step_matcher("re")
# Set logger
LOG = logging.getLogger(__name__)
handler = logging.FileHandler('/var/log/scalarizr.behave.log')
handler.setFormatter(logging.Formatter("%(asctime)-5s %(levelname)-10s- %(message)s", "%d/%m/%y %H:%M"))
#Set  handler&level

LOG.addHandler(handler)
LOG.setLevel(logging.DEBUG)


# unittest assertAlmostEqual replica
def almost_equal(hash, pkg_status, field_name):
    time_fmt = lambda t: float(t.strftime("%y%m%d%H.%M%S"))
    for apkg in pkg_status.get('history', {}):
        if apkg.get('hash', 0) == hash:
            return round(time_fmt(datetime.now())-time_fmt(apkg.get(field_name, 0)), 1) == 0
    else:
        return False


@given(r'I have two available packages from (?P<repo>[\W\w]+)')
def pkg_get_status(context, repo):
    repo = repo.strip()

    pm = context.pkgmgr(repo)
    LOG.info('Set manager for: %s package' % context.platform)
    # Get package list
    pm.updatedb()
    LOG.debug('pm >> updatedb()')
    pm_status = pm.status('mock-scalarizr')
    LOG.debug('pm >> status(mock-scalarizr)')
    LOG.info('result >> %s' % pm_status)
    # Get older and younger packages
    assert len(pm_status['available']) >= 2, "Available packages not valid: %s" % pm_status['available']
    # First package is a pre-candidate, second is a candidate
    first, second = sorted(pm_status['available'], key=LooseVersion)[-2:]
    LOG.info('Package versions are:[younger: %s, older: %s, candidate: %s]' % (second, first, pm_status['candidate']))
    assert pm_status['candidate'] == second,\
        "Available packages not valid, [younger: %s, older: %s, candidate: %s]" % (second, first, pm_status['candidate'])
    # Set global
    # package manager instance
    context.pm = pm
    # pre-candidate version
    context.first = first
    # no version for second package
    context.second = None
    # candidate version
    context.candidate = second


@when(r'I get (?P<pkg>[\W\w]+) package')
def pkg_get(context, pkg):
    # Setup
    pkg_ver = getattr(context, pkg.strip())
    candidate = getattr(context, 'candidate')
    pm = getattr(context, 'pm')

    # Get hash
    dependencies = [{'name': 'mock-scalarizr-ec2', 'version': pkg_ver},
        {'name': 'stab'}] if context.platform != 'Windows' else None
    hash = pm.fetch(
        'mock-scalarizr',
        version=pkg_ver,
        deps=dependencies
    )
    pkg_separator = '-' if context.platform == 'RedHat' else '_'

    if context.platform == 'Windows':
        package_sequence = ['mock-scalarizr{}{}']
    else:
        package_sequence = ['mock-scalarizr-ec2{}{}', 'stab', 'mock-scalarizr{}{}']
    setattr(
        context,
        '%s_sequence' % pkg.strip(),
        map(lambda package: package.format(pkg_separator, pkg_ver or candidate), package_sequence)
    )
    LOG.debug("pm >> fetch(mock-scalarizr, version=%s, "
              "deps=%s" % (pkg_ver, dependencies))
    LOG.info('result >> %s' % hash)
    assert hash, "Can't fetch package: mock-scalarizr %s." % (pkg_ver or candidate)
    # Get status
    pm_status = pm.status('mock-scalarizr')
    LOG.debug('pm >> status(mock-scalarizr)')
    LOG.info('result >> %s' % pm_status)
    # Check results
    assert len([package['hash'] for package in pm_status.get('history', []) if package.get('hash', 0) == hash]) and \
        len(glob(os.path.join(os.environ.get('STORAGE_DIR', '/tmp'), 'mock-scalarizr', hash, '*%s*' % (pkg_ver or candidate)))), \
        'Not found package with valid hash: %s.' % hash
    assert almost_equal(hash, pm_status, 'download_date'), 'No valid "download_date" for the fetched package.'
    setattr(context, '%s_hash' % pkg.strip(), hash)


@then(r'I install (?P<pkg>[\W\w]+) package')
def pkg_install(context, pkg):
    # Setup
    pkg_ver = getattr(context, pkg.strip())
    candidate = getattr(context, 'candidate')
    pm = getattr(context, 'pm')
    hash = getattr(context, '%s_hash' % pkg)
    # Install
    pm.install(hash)
    LOG.debug('pm >> install("%s")' % hash)
    # Check results
    pm_status = pm.status('mock-scalarizr')
    LOG.debug('pm >> status(mock-scalarizr)')
    LOG.info('result >> %s' % pm_status)
    assert (pm_status.get('installed', 0) == (pkg_ver or candidate)) \
        and almost_equal(hash, pm_status, 'install_date'), \
        'Package %s was not installed.' % (pkg_ver or candidate)


@then(r'I check (?P<pkg>[\W\w]+) package installation sequence')
def check_sequence(context, pkg):
    # Get package fetch sequence
    sequence = getattr(context, '%s_sequence' % pkg.strip())
    LOG.info('Fetched packages sequence >> %s' % sequence)
    pm = getattr(context, 'pm')
    hash = getattr(context, '%s_hash' % pkg)
    # Get package install sequence
    with patch('scalarizr.updclient.pkgmgr.system') as system_mock:
        system_mock.return_value = ('', '', 0)
        pm.install(hash)
        args = list(system_mock.call_args[0][0])
        if context.platform != 'Windows':
            LOG.info('Installed packages sequence >> %s' % args[-3:])
        else:
            LOG.info('Installed package >> %s' % args[2])
    # Check sequence
    if context.platform != 'Windows':
        assert all(pkg1 in pkg2 for (pkg1, pkg2) in zip(sequence, args[-3:])), \
            'Installed packages sequence not the same as fetched'
    else:
        assert sequence[0] in args[2]


@then(r'I upgrade first package to the (?P<pkg>[\W\w]+)')
def pkg_upgrade(context, pkg):
    context.execute_steps(u"""
        When I get {0} package
        Then I install {0} package
    """.format(pkg.strip()))


@then(r'I downgrade from the second package to the (?P<pkg>[\W\w]+)')
def pkg_downgrade(context, pkg):
    context.execute_steps(u'Then I install {} package'.format(pkg.strip()))


@then(r'I remove the (?P<pkg>[\W\w]+) package')
def pkg_remove(context, pkg):
    # Setup
    pkg_ver = getattr(context, pkg.strip())
    pm = getattr(context, 'pm')
    # Uninstall
    pm.uninstall('mock-scalarizr')
    LOG.debug('pm >> uninstall("mock-scalarizr")')
    # Check results
    pm_status = pm.status('mock-scalarizr')
    LOG.debug('pm >> status(mock-scalarizr)')
    LOG.info('result >> %s' % pm_status)
    assert pm_status.get('installed', pkg_ver) != pkg_ver, 'Package %s was not uninstalled.' % pkg_ver


# Second scenario


@given(r'I have a scalarizr repository "(?P<repo>[\W\w]+)"')
def setup_repo(context, repo):
    repo = repo.strip()
    pm = context.pkgmgr(repo)
    pm._log_file = pm._log_file.replace('/', '\\')
    context.pkg_name = 'mock-scalarizr'

    LOG.debug('pm >> updatedb()')
    pm.updatedb()
    pm_status = pm.status(context.pkg_name)
    LOG.debug('pm >> status(%s)' % context.pkg_name)
    LOG.debug('result >> %s' % pm_status)

    # 10.0.0.0 is base, 30.0.0.0 is broken and 20.0.0.0 is good
    base, good, bad = sorted(pm_status['available'], key=LooseVersion)[-3:]
    context.pm = pm
    context.base_pkg_ver = base
    context.bad_update_ver = bad
    context.good_update_ver = good


def install_pkg(context, name, version):
    pm = context.pm
    hash_ = pm.fetch(name, version=version)

    pm.install(hash_)
    LOG.debug('pm >> install("%s")' % hash_)

    pm_status = pm.status(name)
    LOG.debug('pm >> status(%s)' % name)
    LOG.debug('result >> %s' % pm_status)
    return pm_status


@given(r'I have installed base version of scalarizr')
def install_base(context):
    pm_status = install_pkg(context, context.pkg_name, context.base_pkg_ver)
    assert (pm_status.get('installed') == context.base_pkg_ver), \
        'Pkgmgr failed to install base package.'


@when(r'it tries to update base with the (?P<version_type>[\W\w]+) version')
def update(context, version_type):
    version_to_install = context.good_update_ver if version_type == 'good' \
        else context.bad_update_ver
    try:
        install_pkg(context, 'mock-scalarizr', version_to_install)
    except context.pkgmgr_exc_class:
        pass


@then(r'it rolls back to the previous vesion')
def check_rollback(context):
    pm_status = context.pm.status(context.pkg_name)
    assert not pm_status.get('installed'), \
            'Pkgmgr failed to ignore bad package.'
    install_pkg(context, 'mock-scalarizr', context.base_pkg_ver)


@then(r'it installs new version of scalarizr')
def check_working(context):
    pm_status = context.pm.status(context.pkg_name)
    assert (pm_status.get('installed') == context.good_update_ver), \
            'Pkgmgr failed to roll back from bad package. Installed: "%s"' % \
            pm_status.get('installed')
