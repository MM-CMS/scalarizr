'''
Created on May 10, 2010

@author: marat
@author: Dmytro Korsakov
'''

from scalarizr.util import disttool, system
import re
import os

class FstoolError(BaseException):
	NO_FS = -100
	CANNOT_MOUNT = -101
	CANNOT_UMOUNT = -102
	CANNOT_CREATE_FS = -103
	
	message = None
	code = None
	
	def __init__(self, *args):
		BaseException.__init__(self, *args)
		self.message = args[0]
		try:
			self.code = args[1]
		except IndexError:
			pass


class Fstab:
	"""
	Wrapper over /etc/fstab
	"""
	LOCATION = None
	filename = None	
	_entries = None
	_re = None
	
	def __init__(self, filename=None):
		self.filename = filename if not filename is None else self.LOCATION
		self._entries = []
		self._re = re.compile("^(\\S+)\\s+(\\S+)\\s+(\\S+)\\s+(\\S+).*$")
		
	def _rescan(self):
		self._entries = []
		f = open(self.filename, "r")
		for line in f:
			if line[0:1] == "#":
				continue
			m = self._re.match(line)
			if m:
				self._entries.append(TabEntry(m.group(1), m.group(2), 
						m.group(3), m.group(4), line.strip()))
		f.close()
		
		
	def list_entries(self, rescan=False):
		if not self._entries or rescan:
			self._rescan()
		return list(self._entries)

	def contains(self, devname=None, mpoint=None, rescan=False):
		eq = dict(devname=devname, mpoint=mpoint)
		for entry in self.list_entries(rescan):
			if self._cmp(entry, eq):
				return True
		return False
			
	def _cmp(self, entry, eq):
		return all(list(getattr(entry, k) == v for k, v in eq.items() if v))	
		
	def find(self, devname=None, mpoint=None, fstype=None, rescan=False):
		eq = dict(devname=devname, mpoint=mpoint, fstype=fstype)
		return list(entry for entry in self.list_entries(rescan) if self._cmp(entry, eq))

	def append(self, devname, mpoint, fstype="auto", options="defaults\t0\t0", autosave=True):
		if not self._entries:
			self._rescan()
		self._entries.append(TabEntry(devname, mpoint, fstype, options))
		if autosave:
			self.save()
	
	def remove(self, devname=None, mpoint=None, fstype=None, rescan=False, autosave=True):
		ent = self.find(devname, mpoint, fstype, rescan)
		if len(ent):
			self._entries.remove(ent[0])
			if autosave:
				self.save()
			return True
		return False
	
	def save(self):
		fp = None
		try:
			fp = open(self.filename, "w")
			fp.write(str(self))
		finally:
			if fp:
				fp.close()
		
	
	def __str__(self):
		return "\n".join(map(str, self._entries)) + "\n"
	

class Mtab(Fstab):
	"""
	Wrapper over /etc/mtab
	"""
	LOCAL_FS_TYPES = None	

		
class TabEntry(object):
	devname = None
	mpoint = None
	fstype = None
	options = None	
	value = None
	
	def __init__(self, devname, mpoint, fstype, options, value=None):
		"""
		@param  str devname:
		@param str mpoint:
		@param str fstype:
		@param str options:
		@param str value: Original fstab line
		"""
		
		self.devname = devname
		self.mpoint = mpoint
		self.fstype = fstype
		self.options = options
		self.value = value
		
	def __str__(self):
		return "%s\t%s\t%s\t%s" % (self.devname, self.mpoint, self.fstype, self.options)

		
if disttool.is_linux():
	Fstab.LOCATION = "/etc/fstab"	
	Mtab.LOCATION = "/etc/mtab"
	Mtab.LOCAL_FS_TYPES = ('ext2', 'ext3', 'xfs', 'jfs', 'reiserfs', 'tmpfs', 'sysfs', 'proc')
	
elif disttool.is_sun():
	Fstab.LOCATION = "/etc/vfstab"	
	Mtab.LOCATION = "/etc/mnttab"
	Mtab.LOCAL_FS_TYPES = ('ext2', 'ext3', 'xfs', 'jfs', 'reiserfs', 'tmpfs', 
		'ufs', 'sharefs', 'dev', 'devfs', 'ctfs', 'mntfs',
		'proc', 'lofs',   'objfs', 'fd', 'autofs')
	

def mount (device, mpoint = '/mnt', options=None, make_fs=False, fstype='ext3', auto_mount=False):
	if not os.path.exists(mpoint):
		os.makedirs(mpoint)
	
	options = " ".join(options or ("-t auto",)) 
	
	if make_fs:
		mkfs(device,fstype)
			
	out = system("mount %(options)s %(device)s %(mpoint)s 2>&1" % vars())[0]
	if out.find("you must specify the filesystem type") != -1:
		raise FstoolError("No filesystem found on device '%s'" % (device), FstoolError.NO_FS)
	
	if options.find("loop") == -1:
		mtab = Mtab()		
		if not mtab.contains(device):
			raise FstoolError("Cannot mount device '%s'. %s" % (device, out), FstoolError.CANNOT_MOUNT)
	
	if auto_mount:
		fstab = Fstab()
		if not fstab.contains(device, mpoint=mpoint, rescan=True):
			fstab.append(device, mpoint)

def umount(device=None, mpoint=None, options=None, clean_fstab = False):
	dev = device or mpoint
	if not os.path.exists(dev):
		raise FstoolError("Path doesn't exists %s" % (dev), FstoolError.CANNOT_UMOUNT)
	
	options = " ".join(options or ())
	
	out, returncode = system("umount %(options)s %(dev)s 2>&1" % vars())[0::2]
	if returncode:
		raise FstoolError("Cannot unmount %s. %s" % (dev, out), FstoolError.CANNOT_UMOUNT)
	
	if clean_fstab:
		fstab = Fstab()
		fstab.remove(device, mpoint)
	
	
def mkfs(device, fstype = 'ext3'):
	out, retcode = system("/sbin/mkfs -t %(fstype)s -F %(device)s 2>&1" % vars())[0::2]
	if retcode:
		raise FstoolError("Cannot create file system on device '%s'. %s" % (device, out), 
				FstoolError.CANNOT_CREATE_FS)
		
def get_mysql_device():
	import string
	o_z = string.ascii_lowercase[14:]
	for letter in o_z:
		device_name = 'dev/sd'+letter
		if not os.path.isfile(device_name):
			return device_name
	return None