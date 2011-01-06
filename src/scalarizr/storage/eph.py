'''
Created on Jan 6, 2011

@author: marat
'''

from . import VolumeConfig, Volume, Snapshot, VolumeProvider, Storage, StorageError
from .util.lvm2 import Lvm2, lvm_group_b64
from .transfer import Transfer

from scalarizr.util import firstmatched
from scalarizr.util.filetool import read_file
from scalarizr.libs.metaconf import Configuration

import subprocess
from random import randint
import hashlib
import logging
import os
import time
import glob
import binascii
import cStringIO


class EphConfig(VolumeConfig):
	type = 'eph'
	vg = None
	lvm_group_cfg = None
	disk = None
	size = None
	path = None
	snap_backend = None

class EphVolume(Volume, EphConfig):
	tranzit_vol = None

class EphSnapshot(Snapshot, EphConfig):
	pass

class EphVolumeProvider(VolumeProvider):
	type = 'eph'
	vol_class = EphVolume
	snap_class = EphSnapshot
	
	_lvm = None
	_snap_pvd = None
	
	def __init__(self):
		self._lvm = Lvm2()
		self._snap_pvd = EphSnapshotProvider()
	
	def _create_layout(self, pv, vg, size):
		''' 
		Creates LV layout
		      [Disk]
		        |
		       [VG]
		      /   \ 
		  [Data] [Tranzit]
		'''

		# Create PV
		self._lvm.create_pv(pv)		

		# Create VG
		if not isinstance(vg, dict):
			vg = dict(name=vg)
		vg_name = vg['name']
		del vg['name']
		vg = self._lvm.create_vg(vg_name, [pv], **vg)
		
		# Create data volume
		lv_kwargs = dict()
		
		size = size or '40%'
		size = str(size)
		if size[-1] == '%':
			lv_kwargs['extents'] = '%sVG' % size
		else:
			lv_kwargs['size'] = int(size)

		data_lv = self._lvm.create_lv(vg, 'data', **lv_kwargs)

		# Create tranzit volume (should be 5% bigger then data vol)
		lvi = self._lvm.lv_info(data_lv)
		size_in_KB = int(read_file('/sys/block/dm-%s/size' % lvi.lv_kernel_minor)) / 2
		tranzit_lv = self._lvm.create_lv(vg, 'tranzit', size='%dK' % (size_in_KB*1.05,))

		return (vg, data_lv, tranzit_lv, size)

	def _destroy_layout(self, vg, data_lv, tranzit_lv):
		# Find PV 
		pv = None
		pvi = firstmatched(lambda pvi: vg in pvi.vg, self._lvm.pv_status())
		if pvi:
			pv = pvi.pv
			
		# Remove storage VG
		self._lvm.change_lv(data_lv, available=False)
		self._lvm.change_lv(tranzit_lv, available=False)
		self._lvm.remove_vg(vg)
		
		if pv:
			# Remove PV if it doesn't belongs to any other VG
			pvi = self._lvm.pv_info(pv)
			if not pvi.vg:
				self._lvm.remove_pv(pv)		
	
	def create(self, **kwargs):
		'''
		@param disk: Physical volume
		@param vg: Uniting volume group
		@param size: Useful storage size (in % of physican volume or MB)
		@param snap_backend: Snapshot backend
		
		Example: 
		Storage.create({
			'type': 'eph',
			'disk': '/dev/sdb',
			'size': '40%',
			'vg': {
				'name': 'mysql_data',
				'ph_extent_size': 10
			},
			'snap_backend': 'cf://mysql_backups/cloudsound/production'
		})
		'''
		if kwargs['lvm_group_cfg']:
			self._lvm.restore_vg(kwargs['vg'], cStringIO.StringIO(kwargs['lvm_group_cfg']))
		else:
			# Create LV layout
			kwargs['vg'], kwargs['device'], tranzit_lv, kwargs['size'] = self._create_layout(
					kwargs['disk'].devname, vg=kwargs.get('vg'), size=kwargs.get('size'))
		
		# Initialize tranzit volume
		kwargs['tranzit_vol'] = Volume(tranzit_lv, '/tmp/sntz' + str(randint(100, 999)), 'ext3', 'base')

		# Accept snapshot backend
		if not isinstance(kwargs['snap_backend'], dict):
			kwargs['snap_backend'] = dict(path=kwargs['snap_backend'])
		
		return super(EphVolumeProvider, self).create(**kwargs)

	def create_from_snapshot(self, **kwargs):
		'''
		...
		@param path: Path to snapshot manifest on remote storage
		
		Example: 
		Storage.create(**{
			'disk' : {
				'type' : 'loop',
				'file' : '/media/storage',
				'size' : 1000
			}
			'snapshot': {
				'type': 'eph',
				'description': 'Last winter mysql backup',
				'path': 'cf://mysql_backups/cloudsound/production/snap-14a356de.manifest.ini'
				'size': '40%',
				'vg': 'mysql_data'
			}
		})
		
		
		'''
		_kwargs = kwargs.copy()
		if not 'snap_backend' in _kwargs:
			_kwargs['snap_backend'] = os.path.dirname(_kwargs['path'])
		vol = self.create(**_kwargs)

		snap = self.snapshot_factory(**kwargs)
		try:
			self._prepare_tranzit_vol(vol.tranzit_vol)
			self._snap_pvd.download(vol, snap, vol.tranzit_vol.mpoint)
			self._snap_pvd.restore(vol, snap, vol.tranzit_vol.mpoint)			
		finally:
			self._cleanup_tranzit_vol(vol.tranzit_vol)
	
		return vol

	def create_snapshot(self, vol, snap):
		try:
			self._prepare_tranzit_vol(vol.tranzit_vol)
			self._snap_pvd.create(vol, snap, vol.tranzit_vol.mpoint)
			return snap
		except:
			self._cleanup_tranzit_vol(vol.tranzit_vol)
			raise
	
	def save_snapshot(self, vol, snap):
		try:
			self._snap_pvd.upload(vol, snap, vol.tranzit_vol.mpoint)
			return snap
		finally:
			self._cleanup_tranzit_vol(vol.tranzit_vol)


	def _prepare_tranzit_vol(self, vol):
		os.makedirs(vol.mpoint)
		vol.mkfs()
		vol.mount()
		
	def _cleanup_tranzit_vol(self, vol):
		vol.umount()
		if os.path.exists(vol.mpoint):
			os.rmdir(vol.mpoint)

	def detach(self, vol, force=False):
		'''
		@type vol: EphVolume
		'''
		super(EphVolumeProvider, self).detach(vol, force)
		if vol.vg:
			vol.lvm_group_cfg = lvm_group_b64(vol.vg)
			self._destroy_layout(vol.vg, vol.devname, vol.tranzit_vol.devname)
			vol.tranzit_vol = None
		vol.disk.detach(force)
		return vol.config()

	def destroy(self, vol, force=False, **kwargs):
		super(EphVolumeProvider, self).destroy(vol, force, **kwargs)

		# Umount tranzit volume
		self._cleanup_tranzit_vol(vol.tranzit_vol)
		
		# Find PV 
		pv = None
		pvi = firstmatched(lambda pvi: vol.vg in pvi.vg, self._lvm.pv_status())
		if pvi:
			pv = pvi.pv
			
		# Remove storage VG
		self._lvm.change_lv(vol.devname, available=False)
		self._lvm.change_lv(vol.tranzit_vol.devname, available=False)
		self._lvm.remove_vg(vol.vg)
		
		if pv:
			# Remove PV if it doesn't belongs to any other VG
			pvi = self._lvm.pv_info(pv)
			if not pvi.vg:
				self._lvm.remove_pv(pv)		

Storage.explore_provider(EphVolumeProvider)


class EphSnapshotProvider(object):

	MANIFEST_NAME 		= 'manifest.ini'
	SNAPSHOT_LV_NAME 	= 'snap'	
	
	chunk_size = None
	'''	Data chunk size in Mb '''

	_logger = None	
	_transfer = None
	_lvm = None
	
	def __init__(self, chunk_size=10):
		self.chunk_size = chunk_size		
		self._logger = logging.getLogger(__name__)
		self._transfer = Transfer()
		self._lvm = Lvm2()
	
	def create(self, volume, snapshot, tranzit_path):
		# Create LVM snapshot
		snap_lv = None
		chunk_prefix = '%s.data' % snapshot.id
		try:
			snap_lv = self._lvm.create_lv_snapshot(volume.devname, self.SNAPSHOT_LV_NAME, extents='100%FREE')
			
			# Copy|gzip|split snapshot into tranzit volume directory
			self._logger.info('Packing volume %s -> %s', volume.devname, tranzit_path) 
			cmd1 = ['dd', 'if=%s' % snap_lv]
			cmd2 = ['gzip', '-1']
			cmd3 = ['split', '-a','3', '-d', '-b', '%sM' % self.chunk_size, '-', '%s/%s.gz.' % 
					(tranzit_path, chunk_prefix)]
			p1 = subprocess.Popen(cmd1, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
			p2 = subprocess.Popen(cmd2, stdin=p1.stdout, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
			p3 = subprocess.Popen(cmd3, stdin=p2.stdout, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
			out, err = p3.communicate()

			if p3.returncode:
				p1.stdout.close()
				p2.stdout.close()				
				p1.wait()
				p2.wait()
				raise StorageError('Error during coping LVM snapshot device (code: %d) <out>: %s <err>: %s' % 
						(p3.returncode, out, err))
		finally:
			# Remove LVM snapshot			
			if snap_lv:
				self._lvm.remove_lv(snap_lv)			
					
		# Make snapshot manifest
		config = Configuration('ini')
		config.add('snapshot/description', snapshot.description, force=True)
		config.add('snapshot/created_at', time.strftime("%Y-%m-%d %H:%M:%S"))
		config.add('snapshot/pack_method', 'gzip') # Not used yet
		for chunk in glob.glob(os.path.join(tranzit_path, chunk_prefix + '*')):
			config.add('chunks/%s' % os.path.basename(chunk), self._md5sum(chunk), force=True)
		
		manifest_path = os.path.join(tranzit_path, '%s.%s' % (snapshot.id, self.MANIFEST_NAME))
		config.write(manifest_path)

		snapshot.path = manifest_path
		snapshot.vg = os.path.basename(volume.vg)
		snapshot.size = volume.size			
		
		return snapshot

	
	def restore(self, volume, snapshot, tranzit_path):
		# Load manifest
		mnf = Configuration('ini')
		mnf.read(os.path.join(tranzit_path, os.path.basename(snapshot.path)))
		
		# Checksum
		for chunk, md5sum_o in mnf.items('chunks'):
			chunkpath = os.path.join(tranzit_path, chunk)
			md5sum_a = self._md5sum(chunkpath)
			if md5sum_a != md5sum_o:
				raise StorageError(
						'Chunk file %s checksum mismatch. Actual md5sum %s != %s defined in snapshot manifest', 
						chunkpath, md5sum_a, md5sum_o)

		# Restore chunks 
		self._logger.info('Unpacking snapshot from %s -> %s', tranzit_path, volume.devname)
		cat = ['cat']
		catargs = list(os.path.join(tranzit_path, chunk) for chunk in mnf.options('chunks'))
		catargs.sort()
		cat.extend(catargs)
		gunzip = ['gunzip']
		dest = open(volume.devname, 'w')
		#Todo: find out where to extract file
		p1 = subprocess.Popen(cat, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
		p2 = subprocess.Popen(gunzip, stdin=p1.stdout, stdout=dest, stderr=subprocess.PIPE)
		out, err = p2.communicate()
		dest.close()
		if p2.returncode:
			p1.stdout.close()
			p1.wait()
			raise StorageError('Error during snapshot restoring (code: %d) <out>: %s <err>: %s' % 
					(p2.returncode, out, err))

	def upload(self, volume, snapshot, tranzit_path):
		mnf = Configuration('ini')
		mnf.read(snapshot.path)
		
		files = [snapshot.path]
		files += [os.path.join(tranzit_path, chunk) for chunk in mnf.options('chunks')]
		
		snapshot.path = self._transfer.upload(files, volume.snap_backend['path'])[0]
		return snapshot

	def download(self, volume, snapshot, tranzit_path):
		# Load manifest
		mnf_path = self._transfer.download(snapshot.path, tranzit_path)[0]
		mnf = Configuration('ini')
		mnf.read(mnf_path)
		
		# Load files
		remote_path = os.path.dirname(snapshot.path)
		files = tuple(os.path.join(remote_path, chunk) for chunk in mnf.options('chunks'))
		self._transfer.download(files, tranzit_path)

	def _md5sum(self, file, block_size=4096):
		fp = open(file, 'rb')
		try:
			md5 = hashlib.md5()
			while True:
				data = fp.read(block_size)
				if not data:
					break
				md5.update(data)
			return binascii.hexlify(md5.digest())
		finally:
			fp.close()

