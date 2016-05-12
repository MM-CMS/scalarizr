import logging
import re

from scalarizr import linux
from scalarizr import storage2
from scalarizr.util import system2

if linux.os.windows:
    from common.utils.winutil import coinitialized_context
    import wmi


LOG = logging.getLogger(__name__)


def _aws_devname_to_disk_drive(device):
    """Returns Win32_DiskDrive (wmi object) instance for corresponding
       aws device name (e.g. 'xvda', '/dev/xvdt').
       MUST BE CALLED AND Win32_DiskDrive RESULT USED ONLY IN coinitialized_context!!!
       Formula from here:
       https://blogs.aws.amazon.com/net/post/Tx3IY716LF05KK6/Stripe-Windows-Ephemeral-Disks-at-Launch
    """
    device = device.split('/')[-1]
    device_suffix = device[3:]

    if 1 == len(device_suffix):
        scsi_id = ord(device_suffix) - 97
    elif 2 == len(device_suffix):
        scsi_id = (ord(device_suffix[0]) - 96) * 26 + ord(device_suffix[1]) - 97
    else:
        raise storage2.StorageError('Wrong AWS disk device name format: {}'.format(device))

    LOG.debug('Searching for disk with device={} and scsi_id={}'.format(device, scsi_id))

    c = wmi.WMI()
    try:
        return c.Win32_DiskDrive(SCSITargetId=scsi_id)[0]
    except (KeyError, IndexError):
        raise storage2.StorageError('Disk device (SCSITargetId == {}) not found'.format(scsi_id))


def get_disk_drive_attribute(device, attr_name):
    """
    Returns attribute value of Win32_DiskDrive which is associated with given AWS device.

    :type device: str
    :param device: Device name in AWS format, i.e. 'xvdb', 'xvdc', etc.

    :type attr_name: str
    :param attr_name: Attribute to take value from
    """
    with coinitialized_context():
        disk = _aws_devname_to_disk_drive(device)
        if disk:
            return disk.__getattr__(attr_name)
        return None


def get_logical_disk_attribute(device, attr_name):
    """
    Returns attribute value of Win32_LogicalDisk which is associated with given AWS device.

    :type device: str
    :param device: Device name in AWS format, i.e. 'xvdb', 'xvdc', etc.

    :type attr_name: str
    :param attr_name: Attribute to take value from
    """
    try:
        # TODO: Should we check amount of partitions on the disk here, and raise
        # exception, if it has more than one partition?
        with coinitialized_context():
            disk = _aws_devname_to_disk_drive(device)
            partition = disk.associators("Win32_DiskDriveToDiskPartition")[0]
            logical_disk = partition.associators("Win32_LogicalDiskToPartition")[0]
            if logical_disk:
                return logical_disk.__getattr__(attr_name)
            return None
    except (KeyError, IndexError):
        LOG.debug('Logical disk not found for device {}'.format(device))
        return None


def aws_bring_disk_online(device):
    """ Brings given disk online, returns True if it has volumes, False otherwise """
    if type(device) is int:
        disk_num = device
    else:
        disk_num = get_disk_drive_attribute(device, 'Index')

    LOG.debug('Bringing device {} with disk number {} online'.format(device, disk_num))

    # diskpart_daemon is needed so diskpart won't change volume number between launches
    # diskpart_daemon = subprocess.Popen("Diskmgmt", shell=True)
    commands = \
        """select disk {}
           online disk noerr
           attributes disk clear readonly noerr
           import noerr
           exit
        """.format(disk_num)
    system2(('diskpart',), stdin=commands)

    commands = \
        """select disk {}
           detail disk
           exit
        """.format(disk_num)
    out, _, _ = system2(('diskpart',), stdin=commands)

    match = re.search(r'Volume[ \t]+[^ \t#]+', out)
    if match:
        vol_num = match.group().split()[-1]
        LOG.debug('Disk has volume {}'.format(vol_num))
        commands = \
            """select volume {}
               remove all noerr
               assign
               exit
            """.format(vol_num)
        system2(('diskpart',), stdin=commands)
        LOG.debug('Automatic drive letter assigned')
        return True
    else:
        LOG.debug('Disk {} ({}) has no volumes'.format(disk_num, device))
        return False
