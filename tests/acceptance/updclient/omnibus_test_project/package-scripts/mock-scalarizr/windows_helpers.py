import ctypes
import _winreg
from ctypes import create_string_buffer, wstring_at
from ctypes import Structure, Union, POINTER
from ctypes import byref, pointer, cast
from ctypes import wintypes


CreateFile = ctypes.windll.kernel32.CreateFileW
GetFinalPathNameByHandle = ctypes.windll.kernel32.GetFinalPathNameByHandleW
CreateFile.restype = ctypes.c_void_p
CloseHandle = ctypes.windll.kernel32.CloseHandle
DeviceIoControl = ctypes.windll.kernel32.DeviceIoControl
GetLastError = ctypes.windll.kernel32.GetLastError
NtQueryDirectoryFile = ctypes.windll.ntdll.NtQueryDirectoryFile
NtQueryDirectoryFile.restype = ctypes.c_ulong


class SymbolicLinkReparseBuffer(Structure):
    _fields_ = [("SubstituteNameOffset", wintypes.USHORT),
                ("SubstituteNameLength", wintypes.USHORT),
                ("PrintNameOffset", wintypes.USHORT),
                ("PrintNameLength", wintypes.USHORT),
                ("Flags", wintypes.ULONG),
                ("PathBuffer", wintypes.WCHAR)]


class MountPointReparseBuffer(Structure):
    _fields_ = [("SubstituteNameOffset", wintypes.USHORT),
                ("SubstituteNameLength", wintypes.USHORT),
                ("PrintNameOffset", wintypes.USHORT),
                ("PrintNameLength", wintypes.USHORT),
                ("PathBuffer", wintypes.WCHAR)]


class GenericReparseBuffer(Structure):
    _fields_ = [("DataBuffer", ctypes.c_ubyte)]


class REPARSE_BUFFER(Union):
    _fields_ = [("SymbolicLink", SymbolicLinkReparseBuffer),
                ("MountPoint", MountPointReparseBuffer),
                ("Generic", GenericReparseBuffer)]


class REPARSE_DATA_BUFFER(Structure):
    _fields_ = [("ReparseTag", wintypes.ULONG),
                ("ReparseDataLength", wintypes.USHORT),
                ("Reserved", wintypes.USHORT),
                ("ReparseBuffer", REPARSE_BUFFER)]


def AsType(ctype, buf):
    ctype_instance = cast(pointer(buf), POINTER(ctype)).contents
    return ctype_instance


def CTL_CODE(deviceType, function, method, access):
    return (((deviceType) << 16) | ((access) << 14) | ((function) << 2) | (method))


SecurityAnonymous = 0
SecurityIdentification = 1
SecurityImpersonation = 2
SecurityDelegation = 3

SECURITY_ANONYMOUS = (SecurityAnonymous << 16)
SECURITY_IDENTIFICATION = (SecurityIdentification << 16)
SECURITY_IMPERSONATION = (SecurityImpersonation << 16)
SECURITY_DELEGATION = (SecurityDelegation << 16)

FILE_SHARE_READ = 0x1
FILE_SHARE_WRITE = 0x2
FILE_SHARE_DELETE = 0x4
FILE_SHARE_VALID_FLAGS = FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE


FILE_READ_DATA = 0x0001
FILE_LIST_DIRECTORY = 0x0001
FILE_WRITE_DATA = 0x0002
FILE_ADD_FILE = 0x0002
FILE_APPEND_DATA = 0x0004
FILE_ADD_SUBDIRECTORY = 0x0004
FILE_CREATE_PIPE_INSTANCE = 0x0004
FILE_READ_EA = 0x0008
FILE_WRITE_EA = 0x0010
FILE_EXECUTE = 0x0020
FILE_TRAVERSE = 0x0020
FILE_DELETE_CHILD = 0x0040
FILE_READ_ATTRIBUTES = 0x0080
FILE_WRITE_ATTRIBUTES = 0x0100

GENERIC_READ = (0x80000000)
GENERIC_WRITE = (0x40000000)
GENERIC_EXECUTE = (0x20000000)
GENERIC_ALL = (0x10000000)


FILE_FLAG_BACKUP_SEMANTICS = 0x02000000
FILE_FLAG_OPEN_REPARSE_POINT = 0x00200000

CREATE_NEW = 1
CREATE_ALWAYS = 2
OPEN_EXISTING = 3
OPEN_ALWAYS = 4
TRUNCATE_EXISTING = 5

if ctypes.sizeof(ctypes.c_void_p) == 8:
    INVALID_HANDLE_VALUE = 0xffffffffffffffff
else:
    INVALID_HANDLE_VALUE = 0xffffffff

DELETE = 0x00010000
READ_CONTROL = 0x00020000
WRITE_DAC = 0x00040000
WRITE_OWNER = 0x00080000

SYNCHRONIZE = 0x100000
STANDARD_RIGHTS_REQUIRED = 0xF0000

STANDARD_RIGHTS_READ = READ_CONTROL
STANDARD_RIGHTS_WRITE = READ_CONTROL
STANDARD_RIGHTS_EXECUTE = READ_CONTROL


# method defs
FILE_DEVICE_FILE_SYSTEM = 0x00000009
METHOD_BUFFERED = 0
FILE_ANY_ACCESS = 0

FSCTL_GET_REPARSE_POINT = CTL_CODE(FILE_DEVICE_FILE_SYSTEM, 42, METHOD_BUFFERED, FILE_ANY_ACCESS)


def raise_windows_error(err):
    raise WindowsError('%s Err: %d' % (wintypes.FormatError(err), err))


def IsReparseTagMicrosoft(tag):
    return bool(tag & 0x80000000)


def open_dir(filename):
    if not isinstance(filename, unicode):
        filename = unicode(filename, 'utf-8')
    handle = CreateFile(filename,
                        FILE_READ_ATTRIBUTES | FILE_LIST_DIRECTORY | SYNCHRONIZE,
                        FILE_SHARE_VALID_FLAGS,
                        None,
                        OPEN_EXISTING,
                        FILE_FLAG_BACKUP_SEMANTICS | FILE_FLAG_OPEN_REPARSE_POINT,
                        None)
    handle = ctypes.c_void_p(handle)
    if handle.value == INVALID_HANDLE_VALUE:
        raise_windows_error(GetLastError())
    return handle


def open_reparse_index(filename):
    if not isinstance(filename, unicode):
        filename = unicode(filename, 'utf-8')
    handle = CreateFile(filename,
                        GENERIC_READ,
                        FILE_SHARE_READ,
                        None,
                        OPEN_EXISTING,
                        FILE_FLAG_BACKUP_SEMANTICS | SECURITY_IMPERSONATION,
                        None)
    handle = ctypes.c_void_p(handle)
    if handle.value == INVALID_HANDLE_VALUE:
        raise_windows_error(GetLastError())
    return handle


def open_file(filename):
    if not isinstance(filename, unicode):
        filename = unicode(filename, 'utf-8')
    handle = CreateFile(filename,
                        GENERIC_READ,
                        FILE_SHARE_VALID_FLAGS,
                        None,
                        OPEN_EXISTING,
                        FILE_FLAG_BACKUP_SEMANTICS | FILE_FLAG_OPEN_REPARSE_POINT,
                        None)
    handle = ctypes.c_void_p(handle)
    if handle.value == INVALID_HANDLE_VALUE:
        raise_windows_error(GetLastError())
    return handle


def get_reparse_point(handle):
    bytesReturned = ctypes.c_ulong(0)
    buffSize = ctypes.sizeof(REPARSE_DATA_BUFFER) + 256
    buff = create_string_buffer(buffSize)
    data_buff = AsType(REPARSE_DATA_BUFFER, buff)
    ret = DeviceIoControl(handle,
                          FSCTL_GET_REPARSE_POINT,
                          None,
                          0,
                          byref(data_buff),
                          buffSize,
                          byref(bytesReturned),
                          None)
    if not ret:
        raise_windows_error(GetLastError())
    return data_buff


def get_symbolic_target(handle):
    data_buff = get_reparse_point(handle)
    offset = REPARSE_DATA_BUFFER.ReparseBuffer.offset + \
        SymbolicLinkReparseBuffer.PathBuffer.offset + \
        data_buff.ReparseBuffer.SymbolicLink.PrintNameOffset - 2 * ctypes.sizeof(ctypes.c_wchar)

    return wstring_at(byref(data_buff, offset),
                      data_buff.ReparseBuffer.SymbolicLink.PrintNameLength / ctypes.sizeof(ctypes.c_wchar))


def check_closed(handle):
    if not CloseHandle(handle):
        raise_windows_error(GetLastError())


def pywin32_update_system():
    """
    use pywin32 to call SendMessageTimeout to send broadcast to all windows.
    """
    import win32gui
    import win32con
    rc, dwReturnValue = win32gui.SendMessageTimeout(
        win32con.HWND_BROADCAST,
        win32con.WM_SETTINGCHANGE,
        0,
        "Environment",
        win32con.SMTO_ABORTIFHUNG, 5000)


def ctypes_update_system():
    """
    use ctypes to call SendMessageTimeout directly
    to send broadcast to all windows.
    """
    SendMessageTimeout = ctypes.windll.user32.SendMessageTimeoutW
    UINT = wintypes.UINT
    SendMessageTimeout.argtypes = wintypes.HWND, UINT, wintypes.WPARAM, wintypes.c_wchar_p, UINT, UINT, UINT
    SendMessageTimeout.restype = wintypes.LPARAM
    HWND_BROADCAST = 0xFFFF
    WM_SETTINGCHANGE = 0x1A
    SMTO_NORMAL = 0x000
    SendMessageTimeout(HWND_BROADCAST, WM_SETTINGCHANGE, 0, 'Environment', SMTO_NORMAL, 10, 0)


def set_HKLM_key(path=None, name=None, value=None, valuetype=_winreg.REG_SZ):
    """
    Set new or existing HKEY_LOCAL_MACHINE key.
    path - a path to reg 'dir' containing a key
    name - key name
    value - key value
    """
    if not all((path, name, value)):
        raise AssertionError('All kwargs should be set')

    _winreg.CreateKey(_winreg.HKEY_LOCAL_MACHINE, path)
    registry_key = _winreg.OpenKey(_winreg.HKEY_LOCAL_MACHINE, path, 0,
                                   _winreg.KEY_WRITE)
    _winreg.SetValueEx(registry_key, name, 0, valuetype, value)
    _winreg.CloseKey(registry_key)
    return True


def get_HKLM_key(path=None, name=None):
    """
    Get a value of existing HKLM key.
    path - a path to reg 'dir' containing a key
    name - key name
    """
    if not all((path, name)):
        raise AssertionError('All kwargs should be set')

    registry_key = _winreg.OpenKey(_winreg.HKEY_LOCAL_MACHINE, path, 0,
                                   _winreg.KEY_READ)
    value, regtype = _winreg.QueryValueEx(registry_key, name)
    _winreg.CloseKey(registry_key)
    return value
