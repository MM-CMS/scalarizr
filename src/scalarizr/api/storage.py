'''
Created on Nov 25, 2011

@author: marat
'''

from scalarizr import rpc, storage2
from scalarizr import bollard
from scalarizr.node import __node__
from scalarizr.util import Singleton


class StorageAPI(object):
    """
    A set of API methods for basic storage management.

    Namespace::

        storage

    StorageAPI methods make use of "volume configuration" dict object, which contains the following:

        - type (Type: string) -- disk type. Required parameter.
        - id (Type: string) -- disk ID.
        - mpoint (Type: string) -- Mount point.
        - fstype (Type: string) Default: "ext3"
    """

    __metaclass__ = Singleton

    error_messages = {
            'empty': "'%s' can't be blank",
            'invalid': "'%s' is invalid, '%s' expected"
    }

    def do_create(self, volume, mkfs, mount, fstab):
        vol = storage2.volume(volume)
        vol.ensure(mkfs=mkfs, mount=mount, fstab=fstab)
        return dict(vol)

    @rpc.command_method
    def create(self, volume=None, mkfs=False, mount=False, fstab=False, async=False):
        """
        Creates a volume from given volume configuration if such volume does not exists.
        Then attaches it to the instance
        and (optionally) creates filesystem and mounts it.

        :type volume: dict
        :param volume: Volume configuration object

        :type mkfs: bool
        :param mkfs: When true method will create filesystem on mounted volume device.
                IF volume already has filesystem no mkfs performed and result volume's "fstype" property updated with existed fstype value

        :type mount: bool
        :param mount: Whether mount volume device.
                Non blank `mpoint` in volume configuration required

        :type fstab: bool
        :param fstab: Whether add device to /etc/fstab

        :type async: bool
        :param async: Execute method in separate thread and report status
                        with Operation/Steps mechanism

        :rtype: dict|string
        """
        self._check_invalid(volume, 'volume', dict)

        async_result = __node__['bollard'].apply_async('api.storage.create',
                                                       args=(volume, mkfs, mount, fstab),
                                                       soft_timeout=(10) * 60,
                                                       hard_timeout=(10 + 1) * 60)
        if async:
            return async_result.task_id
        else:
            return async_result.get()


    def do_snapshot(self, volume, description, tags):
        vol = storage2.volume(volume)
        vol.ensure()
        snap = vol.snapshot(description=description, tags=tags)
        return dict(snap)

    @rpc.command_method
    def snapshot(self, volume=None, description=None, tags=None, async=False):
        """
        Creates a snapshot of a volume.

        :type volume: dict
        :param volume: Volume configuration object

        :type description: string
        :param description: Snapshot description

        :type tags: dict
        :param tags: Key-value tagging. Only 'ebs' and 'gce_persistent'
                volume types support it.

        :type async: bool
        :param async: When True, the method is being executed in a separate thread
                and reports status with Operation/Steps mechanism.
        """
        self._check_invalid(volume, 'volume', dict)
        self._check_empty(volume.get('id'), 'volume.id')
        if description:
            self._check_invalid(description, 'description', basestring)
        if tags:
            self._check_invalid(tags, 'tags', dict)

        async_result = __node__['bollard'].apply_async('api.storage.snapshot',
                                                       args=(volume,),
                                                       soft_timeout=(1 * 24) * 3600,
                                                       hard_timeout=(1 * 24 + 1) * 3600)
        if async:
            return async_result.task_id
        else:
            return async_result.get()

    def do_detach(self, volume, force, **kwds):
        vol = storage2.volume(volume)
        vol.ensure()
        vol.detach(force=force, **kwds)
        return dict(vol)

    @rpc.command_method
    def detach(self, volume=None, force=False, async=False, **kwds):
        """
        Detaches a volume from an instance.

        :type volume: dict
        :param volume: Volume configuration object

        :type force: bool
        :param force: More aggressive.
                - 'ebs' will pass it to DetachVolume
                - 'raid' will pass it to underlying disks

        :type async: bool
        :param async: Execute method in separate thread and report status
                        with Operation/Steps mechanism
        """
        self._check_invalid(volume, 'volume', dict)
        self._check_empty(volume.get('id'), 'volume.id')

        async_result = __node__['bollard'].apply_async('api.storage.detach',
                                                       args=(volume, force), kwds=kwds,
                                                       soft_timeout=(1 * 10) * 60,
                                                       hard_timeout=(1 * 10 + 1) * 60)
        if async:
            return async_result.task_id
        else:
            return async_result.get()

    def do_destroy(self, volume, force, **kwds):
        vol = storage2.volume(volume)
        vol.ensure()
        vol.detach(force=force, **kwds)
        return dict(vol)

    @rpc.command_method
    def destroy(self, volume, force=False, async=False, **kwds):
        """
        Destroys a volume.

        :type volume: dict
        :param volume: Volume configuration object

        :type force: bool
        :param force: More aggressive.
                - 'ebs' will pass it to DetachVolume
                - 'raid' will pass it to underlying disks

        :type async: bool
        :param async: Execute method in separate thread and report status
                        with Operation/Steps mechanism
        """
        self._check_invalid(volume, 'volume', dict)
        self._check_empty(volume.get('id'), 'volume.id')

        async_result = __node__['bollard'].apply_async('api.storage.destroy',
                                                       args=(volume, force), kwds=kwds,
                                                       soft_timeout=(1 * 10) * 60,
                                                       hard_timeout=(1 * 10 + 1) * 60)
        if async:
            return async_result.task_id
        else:
            return async_result.get()

    def do_grow(self, volume, growth):
        vol = storage2.volume(volume)
        growed_vol = vol.grow(**growth)
        return dict(growed_vol)

    @rpc.command_method
    def grow(self, volume, growth, async=False):
        """
        Extends volume capacity.
        Depending on volume type it can be size in GB or number of disks (e.g. for RAID volumes)

        :type volume: dict
        :param volume: Volume configuration object

        :type growth: dict
        :param growth: size in GB for regular disks or number of volumes for RAID configuration.

        Growth keys:

            - size (Type: int, Availability: ebs, csvol, cinder, gce_persistent) -- A new size for persistent volume.
            - iops (Type: int, Availability: ebs) -- A new IOPS value for EBS volume.
            - volume_type (Type: string, Availability: ebs) -- A new volume type for EBS volume. Values: "standard" | "io1".
            - disks (Type: Growth, Availability: raid) -- A growth dict for underlying RAID volumes.
            - disks_count (Type: int, Availability: raid) - number of disks.

        :type async: bool
        :param async: Execute method in a separate thread and report status
                        with Operation/Steps mechanism.

        Example:

        Grow EBS volume to 50Gb::

            new_vol = api.storage.grow(
                volume={
                    'id': 'vol-e13aa63ef',
                },
                growth={
                    'size': 50
                }
            )
        """
        self._check_invalid(volume, 'volume', dict)
        self._check_empty(volume.get('id'), 'volume.id')

        async_result = __node__['bollard'].apply_async('api.storage.grow',
                                                       args=(volume, growth),
                                                       soft_timeout=(1 * 24) * 3600,
                                                       hard_timeout=(1 * 24 + 1) * 3600)
        if async:
            return async_result.task_id
        else:
            return async_result.get()

    def do_replace_raid_disk(self, volume, index, disk):
        vol = storage2.volume(volume)
        vol.replace_disk(index, disk)
        return dict(vol)

    @rpc.command_method
    def replace_raid_disk(self, volume, index, disk, async=False):
        """
        Replace one of the RAID disks (can be retrieved with "status" method) with other.
        Replaced disk will be destroyed.

        :type volume: dict
        :param volume: A volume configuration to replace.

        :type index: int
        :param index: A disk index to replace.

        :type disk: Volume
        :param disk: A replacement disk configuration

        :type async: bool
        :param async: Execute method in a separate thread and report status with Operation/Steps mechanism.
        """
        self._check_invalid(volume, 'volume', dict)
        self._check_invalid(volume, 'index', int)
        self._check_empty(volume.get('id'), 'volume.id')

        async_result = __node__['bollard'].apply_async('api.storage.replace-raid-disk',
                                                       args=(volume, index, disk),
                                                       soft_timeout=(1 * 24) * 3600,
                                                       hard_timeout=(1 * 24 + 1) * 3600)
        if async:
            return async_result.task_id
        else:
            return async_result.get()

    def _check_invalid(self, param, name, type_):
        assert isinstance(param, type_), self.error_messages['invalid'] % (name, type_)

    def _check_empty(self, param, name):
        assert param, self.error_messages['empty'] % name


@bollard.task(name='api.storage.create')
def create(*args, **kwds):
    return StorageAPI().do_create(*args, **kwds)


@bollard.task(name='api.storage.snapshot')
def snapsho(*args, **kwds):
    return StorageAPI().do_snapshot(*args, **kwds)


@bollard.task(name='api.storage.detach')
def detach(*args, **kwds):
    return StorageAPI().do_detach(*args, **kwds)


@bollard.task(name='api.storage.destroy')
def destroy(*args, **kwds):
    return StorageAPI().do_destroy(*args, **kwds)


@bollard.task(name='api.storage.grow')
def grow(*args, **kwds):
    return StorageAPI().do_grow(*args, **kwds)


@bollard.task(name='api.storage.replace_raid_disk')
def replace_raid_disk(*args, **kwds):
    return StorageAPI().do_replace_raid_disk(*args, **kwds)
