from scalarizr.platform import Platform
from common.utils import subprocess2


def get_platform():
    return LxcPlatform()


class LxcPlatform(Platform):
    name = "lxc"

    features = []

    def get_private_ip(self):
        return self.get_public_ip()

    def get_public_ip(self):
        out = subprocess2.check_output(['cat /var/lib/dhcp*/*.eth0.leases | grep fixed | tail -1'],
            shell=True)
        return out.strip().split()[-1][:-1]
