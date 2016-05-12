"""
A set of windows pre- and postinstall actions.
"""
import argparse
import glob
import logging
import os
import re
import shutil
import subprocess
import _winreg
from logging import config


from windows_helpers import (
    get_symbolic_target,
    open_dir,
    check_closed,
    pywin32_update_system,
    ctypes_update_system,
    set_HKLM_key, get_HKLM_key
)


parser = argparse.ArgumentParser()
parser.add_argument('--app_root', dest='APP_ROOT',
                    help="""
a location where all application's VERSION-specific dirs are stored:\n
APP_ROOT/\n
        /CURRENT\n
        /1.2.3\n
        /4.5.6\n
        /etc'\n
""")
parser.add_argument("--projectlocation", dest="PROJECTLOCATION",
                    help="An exact install location: APP_ROOT\\version. See --app_root.")
parser.add_argument("--version", dest="VERSION", help="A version we are installing right now")
parser.add_argument("--phase", dest="PHASE", help="Installation phase: pre, post")
args = parser.parse_args()

if args.APP_ROOT:
    APP_ROOT = args.APP_ROOT.strip().strip("'")
elif args.PROJECTLOCATION:
    PROJECTLOCATION = args.PROJECTLOCATION.strip().strip("'").strip("\\")
    APP_ROOT = os.path.split(PROJECTLOCATION)[0]
VERSION = args.VERSION.strip().strip("'")
PHASE = args.PHASE.strip().strip("'")

# a path to directory symlink pointing to actuall app installation
CURRENT = os.path.abspath(os.path.join(APP_ROOT, 'current'))
LOGDIR = os.path.abspath(os.path.join(APP_ROOT, 'var/log'))
PROCDIR = os.path.abspath(os.path.join(APP_ROOT, 'var/run'))
LOGFILE = os.path.abspath(os.path.join(LOGDIR, 'installscripts-{0}-log.txt'.format(VERSION)))
ETC_BACKUP_DIR = os.path.abspath(os.path.join(APP_ROOT, 'etc_bkp'))
LEGACY_INSTALLDIR = 'C:\\Program Files\\Scalarizr'
STATUS_FILE = os.path.abspath(os.path.join(APP_ROOT, 'service_status'))
# Logging will not create log directories, C:\var\log should exist
for dir_ in (LOGDIR, PROCDIR):
    if not os.path.exists(dir_):
        os.makedirs(dir_)


log_settings = {
    'version': 1,
    'handlers': {
        'file': {
            'class': 'logging.handlers.RotatingFileHandler',
            'level': 'INFO',
            'formatter': 'detailed',
            'filename': LOGFILE,
        },

    },
    'formatters': {
        'detailed': {
            'format': '%(asctime)s %(levelname)-4s %(message)s',
            'datefmt': '%Y-%m-%d %H:%M:%S'
        },
    },
    'loggers': {
        'extensive': {
            'level': 'DEBUG',
            'handlers': ['file', ]
        },
    }
}
logging.config.dictConfig(log_settings)
LOG = logging.getLogger('extensive')
# we'll put some computations in a simple cache
cache = {}


def main():
    """
    Script entry point.
    """
    try:
        LOG.info(
            'Processing the following command call:'
            '\n {0}\\embedded\\python.exe {0}\\embedded\\installscripts\\msi_install_actions.py '
            '--phase="{1}" --projectlocation="{0}" --version="{2}"'.format(
                PROJECTLOCATION,
                PHASE,
                VERSION
            ))
        LOG.info('Starting {0}install sequense'.format(PHASE))
        if PHASE in ('pre', 'preinstall'):
            preinstall_sequence()
        if PHASE in ('post', 'postinstall'):
            postinstall_sequence()
    except Exception, e:
        LOG.exception(e)


def preinstall_sequence():
    """
    A set of acions to run after InstallInitialize.
    """
    LOG.info('Deleting artifacts left from previous scalarizr installations in {0}'.format(APP_ROOT))
    delete_previous()
    stop_szr()


def postinstall_sequence():
    """
    A set of actions to run after InstallFinalize.
    """

    stop_szr()
    copy_configuration()
    mock_legacy_install()
    uninstall_legacy_package()
    relink()
    create_bat_files()
    add_binaries_to_path()
    open_firewall()
    install_winservices()
    set_servicescpecific_envvars()
    send_wmisettingschangesignal()
    start_szr()
    rm_r(ETC_BACKUP_DIR)


def uninstall_legacy_package():
    """
    If we are doing second-after-migration install and scalarizr was already running
    before this install, then we can safely uninstall legacy package
    """
    legacy_uninstaller = abspath_join(LEGACY_INSTALLDIR, 'uninst.exe')
    if os.path.exists(legacy_uninstaller) and os.path.exists(CURRENT) and cache.get('szr_status') is not None:
        LOG.info("Legacy package found and package migration was alredy completed. Uninstalling legacy package.")
        subprocess.check_call("\"{0}\" /S".format(legacy_uninstaller), shell=True)


def mock_legacy_install():
    """
    In case we are doing a migration, a LEGACY_INDTALLDIR/src directory should be created,
    in order legacy update script could confirm that new files were added during install.
    """
    legacy_src = abspath_join(LEGACY_INSTALLDIR, 'src')
    if os.path.exists(LEGACY_INSTALLDIR) and not os.path.exists(legacy_src):
        os.makedirs(legacy_src)


def delete_previous():
    """
    Delete all directories inside scalarizr location
    except one that is symlinked to CURRENT
    """
    LOG.info('Cleaning up application root')
    if not os.path.exists(CURRENT):
        return
    real_location = realpath(CURRENT)
    LOG.info('{0} points to {1}'.format(CURRENT, real_location))
    confdir = abspath_join(APP_ROOT, 'etc')
    vardir = abspath_join(APP_ROOT, 'var')
    for location in glob.glob(APP_ROOT + '\\*'):
        if location in (real_location, CURRENT, vardir, confdir, ETC_BACKUP_DIR):
            LOG.debug('leaving {0} intact'.format(location))
            continue
        LOG.info('\t\t- Removing {0}'.format(location))
        rm_r(location)


def unlink_current():
    """
    Unlink CURRENT application VERSION
    """
    if os.path.exists(CURRENT):
        LOG.info('\t\t- Removing {0}'.format(CURRENT))
        subprocess.check_call('rmdir "{0}"'.format(CURRENT), shell=True)


def link_current(target):
    """
    Create link for the VERSION that is being installed.
    """
    LOG.info('\t\t- Linking {0} to {1}'.format(CURRENT, target))
    subprocess.check_call('cmd.exe /c mklink /D "{0}" "{1}"'.format(CURRENT, target), shell=True)


def relink():
    LOG.info('Resetting "APP_ROOT\\current" symlink target.')
    unlink_current()
    new = abspath_join(APP_ROOT, VERSION)
    link_current(new)


def add_binaries_to_path():
    """
    Add application's binary files location to a
    systemwide PATH environment variable.
    """
    LOG.info('Adding {0}\\bin to current user\'s PATH environment variable'.format(CURRENT))
    binaries = ";".join([abspath_join(CURRENT, 'bin'), APP_ROOT])
    if not on_path(binaries):
        add_to_path(binaries)


def create_bat_files():
    """
    Create .bat files  with a deprecation notification
    in a root installation dir.
    """
    LOG.info('Creating .bat files in {0}'.format(APP_ROOT))
    binaries = abspath_join(CURRENT, 'bin')
    contents = 'echo "{0}.bat is deprecated you can use {0} instead"\nstart /d \"%s\" {0}.exe\npause' % (binaries)
    for executable in ('scalarizr', 'szradm', 'scalr-upd-client'):
        bat_location = abspath_join(APP_ROOT, '{0}.bat'.format(executable))
        with open(bat_location, 'w+') as fp:
            fp.write(contents.format(executable))


def copy_configuration():
    """
    Copy configuration files from previous installation(if any)
    into new installation and then into APP_ROOT/etc.
    Overwrite when copying to APP_ROOT/VERSION/etc.
    Do not overwrite when copying to APP_ROOT/etc.
    """
    legacy_conf_dir = abspath_join(LEGACY_INSTALLDIR, "etc")
    new_conf_dir = abspath_join(APP_ROOT, VERSION, 'etc')
    root_conf_dir = abspath_join(APP_ROOT, 'etc')
    if os.path.exists(CURRENT):
        previous_conf_dir = abspath_join(realpath(CURRENT), 'etc')
        if os.path.exists(ETC_BACKUP_DIR):
            cp_r(ETC_BACKUP_DIR, new_conf_dir, overwrite=True)  # first get configs from backup dir
        cp_r(previous_conf_dir, new_conf_dir, overwrite=True)  # then copy from previous symlink target
    elif os.path.exists(legacy_conf_dir):
        cp_r(legacy_conf_dir, root_conf_dir)
    cp_r(new_conf_dir, root_conf_dir)


def cp_r(source, destination, overwrite=False):
    if overwrite:
        flags = '/E'
        info = 'Overwriting existing'
    else:
        flags = '/E /XN /XO /XC'
        info = 'No overwrite'

    LOG.info('Copying files and dirs from {0} to {1}. {2}'.format(source, destination, info))
    if source == destination:
        LOG.debug('\t\t- Source and destination match. Skipping action.')
        return
    if not os.path.exists(source):
        LOG.info('\t\t- {0} does not exist. Skipping action.'.format(source))
        return
    # robocopy will want you to say 'from sourcedir to destdir copy thefile': 'robocopy source dest file'
    source, thefile, flags = (os.path.split(source) + ('',)) if os.path.isfile(source) else (source, '', flags)
    if not os.path.exists(destination):
        LOG.debug('\t\t- {0} does not exist. Creating.'.format(source))
        os.makedirs(destination)
    subprocess.call(
        'C:\\Windows\\System32\\Robocopy.exe "{0}" "{1}" {2} {3}'.format(source, destination, thefile, flags))


def open_firewall():
    """
    Open firewall ports 8008-8014
    """
    LOG.info('Opening firewall ports 8008-8014')
    subprocess.check_call(
        'C:\\Windows\\system32\\netsh.EXE '
        'advfirewall firewall add rule name=Scalarizr '
        'dir=in protocol=tcp localport=8008-8014 action=allow',
        shell=True)


def install_winservices():
    subprocess.check_call('"{0}\\bin\\scalarizr.exe" --install-win-services'.format(CURRENT), shell=True)
    subprocess.check_call('"{0}\\bin\\scalr-upd-client.exe" --startup auto install'.format(CURRENT), shell=True)


def start_szr():
    """
    Start scalarizr if it was previously stopped.
    """
    if cache.get('szr_status') == 'was_stopped' or os.path.exists(STATUS_FILE):
        LOG.info('Starting scalarizr, as it was previously stopped by this script')
        powershell_run('Start-Service Scalarizr')
        rm_r(STATUS_FILE)


def stop_szr():
    """
    Stop scalarizr if it is running.
    """
    LOG.info('Checking scalarizr is not running.')
    services = powershell_run('Get-Service')
    if 'Scalarizr' in services:
        LOG.info('\t\t- Found scalarizr in services list')
        if powershell_run('(Get-Service -Name Scalarizr).Status') == 'Running':
            LOG.info('\t\t- Scalarizr is running. Stopping...')

            powershell_run('Stop-Service Scalarizr')

            cache['szr_status'] = 'was_stopped'
            LOG.info('\t\t- Stopped scalarizr')
            with open(STATUS_FILE, 'w+') as fp:
                fp.write('was_stopped')
        else:
            LOG.info('\t\t- scalarizr is not running')
            cache['szr_status'] = 'not_running'


def set_servicescpecific_envvars():
    """
    Create service-scpecific environment to provide pythonpath for scalarizr services.
    """
    LOG.info('Creating service-wide environment to provide pythonpath for Scalarizr services.')
    path = "SYSTEM\\CurrentControlSet\\Services\\{service_name}"
    service_env = "PYTHONPATH=\"{0}\\src;{0}\\embedded\\Lib;{0}\\embedded\\Lib\\site-packages\""
    for name in ('Scalarizr', 'ScalrUpdClient'):
        set_HKLM_key(
            path=path.format(service_name=name),
            name='Environment',
            value=[service_env.format(CURRENT)],
            valuetype=_winreg.REG_MULTI_SZ
        )


def powershell_run(command):
    """
    Execute oneliner via powerhell
    """
    powershell = 'C:\\WINDOWS\\system32\\WindowsPowerShell\\v1.0\\Powershell.exe -Command'
    return subprocess.check_output('{0} {1}'.format(powershell, command), shell=True)


def abspath_join(*args):
    """
    Shortcut conveniece for absolute path.
    """
    return os.path.abspath(os.path.join(*args))


def add_to_path(path):
    """
    Add path argument to System-wide PATH environment variable.
    Persistent change.
    """
    key_dir = 'SYSTEM\\CurrentControlSet\\Control\\Session Manager\\Environment'
    current_path = get_HKLM_key(path=key_dir, name='Path')
    bin_locations = []
    for location in current_path.split(";"):
        if 'scalarizr' in location.lower():
            continue
        # expand %% variables into strings, as windows registry will not expand
        # programmatic(nonGUI) editions
        location = re.sub("%\w+%", lambda m: os.path.expandvars(m.group()), location)
        bin_locations.append(location)

    bin_locations.append(path)
    new_path = ";".join(bin_locations)
    set_HKLM_key(path=key_dir, name='Path', value=new_path)


def on_path(path):
    """
    Check if a dir is on path.
    """
    key_dir = 'SYSTEM\\CurrentControlSet\\Control\\Session Manager\\Environment'
    current_path = get_HKLM_key(path=key_dir, name='Path')
    return path + ";" in current_path or current_path.endswith(path)


def send_wmisettingschangesignal():
    """
    Apply registry key changes immediatley.
    """
    LOG.info("Updating environment via WM_SETTINGCHANGE signal")
    try:
        pywin32_update_system()
    except ImportError:
        ctypes_update_system()


def realpath(path):
    """
    get_symbolic_target for win
    """
    try:
        import win32file
        f = win32file.CreateFile(
            path,
            win32file.GENERIC_READ,
            win32file.FILE_SHARE_READ,
            None,
            win32file.OPEN_EXISTING,
            win32file.FILE_FLAG_BACKUP_SEMANTICS,
            None
        )
        target = win32file.GetFinalPathNameByHandle(f, 0)
        # an above gives us something like u'\\\\?\\C:\\tmp\\scalarizr\\3.3.0.7978'
        return target.strip('\\\\?\\')
    except ImportError:
        handle = open_dir(path)
        target = get_symbolic_target(handle)
        check_closed(handle)
        return target


def rm_r(path):
    """
    Recursively remove given path. Do not complain if path abscent.
    """
    if not os.path.exists(path):
        return
    try:
        os.remove(path)
    except OSError:
        try:
            os.rmdir(path)
        except (OSError, WindowsError):
            shutil.rmtree(path)


if __name__ == '__main__':
    main()
