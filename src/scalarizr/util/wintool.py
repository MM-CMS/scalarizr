import _winreg as winreg
import logging
import os
import tempfile
import time

import requests

from common.utils import subprocess2


LOG = logging.getLogger(__name__)


class RebootExpected(Exception): 
    pass

def read_hklm_key(sub_key):
    try:
        return winreg.OpenKey(winreg.HKEY_LOCAL_MACHINE, sub_key, 0, winreg.KEY_READ)
    except WindowsError as e:
        if e.winerror == 2:
            LOG.debug('{} not found in registry'.format(sub_key))
        else:
            raise

def poll_registry_key(key, value_name, value):
    values = set()
    with key:
        LOG.debug('Waiting %s: %s ...', value_name, value)
        while True:
            curvalue = winreg.QueryValueEx(key, value_name)[0]
            values.add(curvalue)
            if curvalue == value:
                LOG.debug('Reached %s: %s', value_name, value)
                return values
            time.sleep(2)

def wait_boot():
    key = read_hklm_key(r'SOFTWARE\Microsoft\Windows\CurrentVersion\Setup\State')
    observed_states = poll_registry_key(key, 'ImageState', 'IMAGE_STATE_COMPLETE')
    LOG.info('Windows is ready!')
    if len(observed_states) > 1:
        # when value transitions observed, pending reboot is expected 
        LOG.info('Pending reboot is expected')
        LOG.debug('  setup states: %s', tuple(observed_states))
        raise RebootExpected()     


def install_vc2010_redist():
    r = requests.get(
        'https://download.microsoft.com/download/3/2/2/3224B87F-CFA0-4E70-BDA3-3DE650EFEBA5/vcredist_x64.exe', 
        stream=True)
    r.raise_for_status()
    fd, vcredist = tempfile.mkstemp(suffix='.exe')
    try:
        # download installer
        with os.fdopen(fd, 'wb') as fp:
            for buf in r.iter_content(4096):
                fp.write(buf)
        # run vcredist_x64 installer
        subprocess2.check_output((vcredist, '/q', '/norestart'))
    finally:
        os.unlink(vcredist)
