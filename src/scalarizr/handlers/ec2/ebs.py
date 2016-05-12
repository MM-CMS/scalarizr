'''
Created on Mar 1, 2010

@author: marat
'''

from xml.etree import ElementTree
import logging

from scalarizr.bus import bus
from scalarizr import linux
from scalarizr.handlers.block_device import BlockDeviceHandler
from scalarizr.storage2.volumes import ebs


LOG = logging.getLogger(__name__)


def get_handlers():
    return [EbsHandler()]

class EbsHandler(BlockDeviceHandler):

    def __init__(self):
        BlockDeviceHandler.__init__(self, 'ebs')
        bus.on(init=self.on_ebs_init)

    def get_devname(self, devname):
        return ebs.device2name(devname)

    def on_ebs_init(self):
        if linux.os.windows:
            self._disable_ec2config_drive_plugins()

    def _disable_ec2config_drive_plugins(self):
        LOG.debug('Disabling ec2config drive plugins')

        config_path = r'C:\Program Files\Amazon\Ec2ConfigService\Settings\config.xml'
        with open(config_path, 'r') as fp:
            raw = fp.read()
        if not raw:
            return

        config_xml = ElementTree.fromstring(raw)
        for plugin in config_xml.findall('./Plugins/'):
            name = plugin.find('Name')
            if name is not None and name.text in ['Ec2SetDriveLetter', 'Ec2InitializeDrives']:
                state = plugin.find('State')
                state.text = 'Disabled'

        with open(config_path, 'w') as fp:
            fp.write(ElementTree.tostring(config_xml))

        linux.system(('net', 'stop', 'ec2config'), raise_exc=False)
        linux.system(('net', 'start', 'ec2config'), raise_exc=False)
