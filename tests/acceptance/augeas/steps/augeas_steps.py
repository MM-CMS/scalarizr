# coding: utf-8

"""
Created on 12.01.2014
@author: Eugeny Kurkovich
"""

import os
import subprocess
import logging
from behave import *
from augeas import Augeas

use_step_matcher("re")
LOG = logging.getLogger(__name__)


@given(r'I have a sample (?P<service>[\w\W]+) configuration')
def create_sample_config(context, service):
    service = service.strip()
    # Create parser
    parser = Augeas(
        root=getattr(context, 'AUGEAS_ROOT'),
        loadpath=os.path.join(getattr(context, 'AUGEAS_LENS_LIB'), '{}_lens'.format(service)),
        flags=1 << 0)
    LOG.info('Created augeas instance for parsing %s service.' % service)
    # Get config path from lens
    key = '/augeas/load/{}/incl'.format(service.capitalize())
    LOG.info('augtool> get %s' % key)
    res = parser.get(key)
    LOG.info('result > %s' % res)
    assert res, '{} lens was not load properly.'.format(service.capitalize())
    # Create sample config
    config_path = res[1:] if res.startswith('/') else res
    setattr(context, 'config_path', config_path)
    sample_config = os.path.join(getattr(context, 'AUGEAS_ROOT'), config_path)
    if not os.path.exists(os.path.dirname(sample_config)):
        os.makedirs(os.path.dirname(sample_config))
        with open(sample_config, "w") as f:
            f.write(context.text)
    assert os.path.exists(sample_config) or not os.path.getsize(sample_config),\
        "Can't create sample config: {}".format(sample_config)
    LOG.info('Config %s was successfully created' % sample_config)
    setattr(context, 'parser', parser)
    setattr(context, 'service', service)


@when(r'I parse it')
def parse_sample_config(context):
    service = getattr(context, 'service')
    kwargs = {
        'env': {
            'AUGEAS_ROOT': getattr(context, 'AUGEAS_ROOT'),
            'AUGEAS_LENS_LIB': os.path.join(getattr(context, 'AUGEAS_LENS_LIB'), '{}_lens'.format(service))
        },
        'stdin': subprocess.PIPE,
        'stdout': subprocess.PIPE,
        'stderr': subprocess.PIPE,
        'close_fds': True,
        'shell': True
    }
    key = '/files/{}'.format(getattr(context, 'config_path'))
    LOG.info('augtool> print %s' % key)
    out, err = subprocess.Popen('augtool', **kwargs).communicate('print {}'.format(key))
    LOG.info('result>\n%s' % out)
    assert out or not err, 'Config "%s" was not parsed properly.%s' % (key, '\nErr: %s' % err if err else '')
    parsed_conf = [line.replace(key, '').replace('\\\\', '\\').strip()[1:] for line in out.split('\n')][1:-1]
    LOG.info('Sample config was parsed to tree: %s' % parsed_conf)
    setattr(context, 'parsed_conf', parsed_conf)


@then(r'I get such tree')
def check_parsed_config(context):
    parsed_conf = getattr(context, 'parsed_conf')
    ref_conf = context.text.replace('\\\\', '\\').split('\n')
    LOG.info('Reference tree is: %s' % ref_conf)
    diff = lambda l1, l2: list(set(l2)-set(l1))
    assert parsed_conf == ref_conf, 'Parsed result not match, {} not in {}'.format(
        diff(ref_conf, parsed_conf),
        ref_conf)
    LOG.info('Parsed result matched.')


@when(r'I change some options in sample config to')
def change_sample_config(context):
    changes_list = context.text.split('\n')
    parser = getattr(context, 'parser')
    config_path = getattr(context, 'config_path')
    LOG.info('Change sample config')
    parser.load()
    LOG.info('augtool> load')
    for changes in changes_list:
        node_path, value = changes.strip().split('=')
        key = '/files/{}/{}'.format(config_path, node_path.strip())
        value = (value.strip().startswith('"') and value.strip().endswith('"')) and value.strip()[1:-1] or value.strip()
        LOG.info('augtool> set %s %s' % (key, value))
        parser.set(key, value)
    LOG.info('augtool> save')
    parser.save()
    backup = os.path.join(getattr(context, 'AUGEAS_ROOT'), '%s.augsave' % config_path)
    assert os.path.exists(backup), 'Changes was not saved in sample config, bakup %s not found ' % backup
    LOG.info('Sample config changed. Created backup: %s' % backup)
