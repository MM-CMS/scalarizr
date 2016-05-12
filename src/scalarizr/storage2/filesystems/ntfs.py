import logging

from scalarizr import storage2
from scalarizr.storage2 import filesystems
from scalarizr.util import system2
from scalarizr import linux

if linux.os.windows:
    from common.utils.winutil import coinitialized
    from scalarizr.storage2.util import wintools
    import wmi


LOG = logging.getLogger(__name__)


class NTFSException(storage2.StorageError):
    """ Error occured while trying to make fs"""


class NTFSFileSystem(filesystems.FileSystem):
    type = 'ntfs'

    features = {
            'freezable': False,
            'resizable': False,
            'umount_on_resize': False
    }

    def __init__(self):
        pass

    def mkfs(self, device, *_):
        """ device is either Windows scsi id or disk number """
        if type(device) is int:
            disk_num = device
        else:
            disk_num = wintools.get_disk_drive_attribute(device, 'Index')

        LOG.debug('Creating filesystem on {} ({})'.format(disk_num, device))
        commands = \
            """select disk {}
               online disk noerr
               attributes disk clear readonly noerr
               clean
               convert dynamic noerr
               create volume simple
               format fs=ntfs quick
               assign
               exit
            """.format(disk_num)
        out, _, _ = system2(('diskpart',),
            stdin=commands,
            error_text=self.error_messages['mkfs'] % device)
        if "The disk you specified is not valid" in out:
            raise NTFSException("Diskpart can't find disk %s" % disk_num)
        if "The volume you selected is not valid or does not exist" in out:
            raise NTFSException("Error occured while creating volume")

    def resize(self, device, size=None, *short_args, **long_kwds):
        pass

    def _get_volume_by_drive_letter(self, drive_letter):
        """
        Temporary: get Win32_Volume by assigned DriveLetter.
        Will use Win32_DiskDrive.SCSITargetId instead of DriveLetter in future.
        DON'T USE IT OUTSIDE OF @coinitialized CONTEXT
        """
        w = wmi.WMI()
        try:
            return w.Win32_Volume(DriveLetter="{}:".format(drive_letter))[0]
        except IndexError:
            raise storage2.Win32VolumeNotFound(drive_letter)

    @coinitialized
    def set_label(self, drive_letter, label):
        vol = self._get_volume_by_drive_letter(drive_letter)
        vol.Label = label

    @coinitialized
    def get_label(self, drive_letter):
        vol = self._get_volume_by_drive_letter(drive_letter)
        return vol.Label


storage2.filesystem_types[NTFSFileSystem.type] = NTFSFileSystem
