
import os
import sys
import urllib2

from common.utils.facts import fact
from scalarizr.node import __node__
from scalarizr import storage2
from scalarizr.storage2.volumes import base
from scalarizr.storage2.volumes import ebs


class Ec2EphemeralVolume(ebs.WinMountMixin, base.Volume):

    def __init__(self, name=None, **kwds):
        '''
        :type name: string
        :param name: Ephemeral disk name. Valid values: 'ephemeral{0-3}'
                On EC2 up to 4 ephemeral devices may be available on instance.
                It depends from instance type.
        '''
        super(Ec2EphemeralVolume, self).__init__(name=name, **kwds)
        self.features.update({
                'restore': False,
                'detach': False
        })

    def _ensure(self):
        self._check_attr('name')
        try:
            url = 'http://169.254.169.254/latest/meta-data/block-device-mapping/%s' % self.name
            device = urllib2.urlopen(url).read().strip()
        except:
            msg = "Failed to fetch device name for instance store '%s'. %s (%s)" % (
                            self.name, sys.exc_info()[1], url)
            raise storage2.StorageError, msg, sys.exc_info()[2]
        else:
            device = ebs.name2device(device)
            if fact['os']['name'] != 'windows':
                if not os.path.exists(device):
                    raise Exception((
                        "Instance store device {} ({}) doesn't exist. "
                        "Please check that instance type {} supports it").format(
                            device, self.name, __node__['platform'].get_instance_type()))
            self.device = device


    def _snapshot(self):
        raise NotImplementedError()


storage2.volume_types['ec2_ephemeral'] = Ec2EphemeralVolume
