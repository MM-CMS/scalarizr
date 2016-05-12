"""
A set of windows pre- and postinstall actions.
"""
import argparse
import errno
import glob
import logging
import os
import shutil
import subprocess


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
LOGFILE = os.path.abspath(os.path.join(LOGDIR, 'installscripts-{0}-log.txt'.format(VERSION)))
# Logging will not create log directories, C:\var\log should exist
if not os.path.exists(LOGDIR):
    os.makedirs(LOGDIR)
logging.basicConfig(filename=LOGFILE, level=logging.DEBUG)
LOG = logging.getLogger(__name__)
# we'll put some computations in a simple cache
cache = {}


def cached(function):
    """
    A convenience function to store results of repeatedly called methods, returning
    a constant value.
    """
    def cached_inner():
        fname = function.__name__
        if fname in cache:
            return cache[fname]
        else:
            cache[fname] = function()
            return cache[fname]
    return cached_inner


def main():
    """
    Script entry point.
    """
    try:
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

    if installation_type() == 'upgrade':
        LOG.info('Deleting previous versions')
        delete_previous()
    LOG.info('Nothing to do in preinstall')


def postinstall_sequence():
    """
    A set of actions to run after InstallFinalize.
    """
    if installation_type() == 'upgrade':
        copy_configuration()
        stop_szr()
    relink()
    create_bat_files()
    add_root_dirs()
    add_binaries_to_path()
    open_firewall()
    send_wmisettingschangesignal()
    install_winservices()
    start_szr()


@cached
def installation_type():
    """
    Check dir tree inside APP_ROOT and define, what
    is going on right now:
    If we have
        APP_ROOT/
            CURRENT/
            1.2.3.4/
    then it's an upgrade.
    If we have
        APP_ROOT/
            etc/
            ....
            scripts/
            share/
            src/
    then its a special case of switching package types.

    If we have an empty root dir or no root dir present
    then it's a fresh install.
    """
    if os.path.exists(abspath_join(APP_ROOT, 'scripts')):
        LOG.info('Found a legacy application root directory structure.')
        return 'migration.omnibus'
    if os.path.exists(CURRENT):
        LOG.info('{0} already exists.'.format(CURRENT))
        return 'upgrade'
    elif os.path.exists(APP_ROOT) and os.listdir(APP_ROOT) != []:
        LOG.info('Looks like we are doing a fresh install, but {0} is not empty'.format(APP_ROOT))
    else:
        LOG.info('Doing a fresh install')
    return 'install'


def delete_previous():
    """
    Delete all dir inside scalarizr location
    except the one that is symlinked to CURRENT
    """
    LOG.info('Cleaning up application root')
    real_location = realpath(CURRENT)
    LOG.info('{0} maps to {1}'.format(CURRENT, real_location))
    confdir = abspath_join(APP_ROOT, 'etc')
    logdir = abspath_join(APP_ROOT, 'log')
    for location in glob.glob(APP_ROOT + '\\*'):
        if location in (real_location, CURRENT, logdir, confdir):
            LOG.debug('leaving {0} intact'.format(location))
            continue
        LOG.info('Removing {0}'.format(location))
        rm_r(location)


def unlink_current():
    """
    Unlink CURRENT application VERSION
    """
    if os.path.exists(CURRENT):
        subprocess.check_call('rmdir "{0}"'.format(CURRENT), shell=True)


def link_current(target):
    """
    Create link for the VERSION that is being installed.
    """
    subprocess.check_call('cmd.exe /c mklink /D "{0}" "{1}"'.format(CURRENT, target), shell=True)


def relink():
    LOG.info('Relinking')
    unlink_current()
    new = abspath_join(APP_ROOT, VERSION)
    link_current(new)


def add_conf_dir():
    """
    add APP_ROOT/etc
    """
    confdir = abspath_join(APP_ROOT, 'etc')
    if not os.path.exists(confdir):
        os.makedirs(confdir)


def add_binaries_to_path():
    """
    Add application's binary files location to a
    systemwide PATH environment variable.
    """
    LOG.info('Adding binaries to path')
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
    Copy configuration files from previous installation into
    new installation and then into
    application's root confs directory.
    Overwrite when copying to new.
    Do not overwrite when copying to root.
    """
    previous = abspath_join(realpath(CURRENT), 'etc')
    cache['previous_symlink_target'] = previous  # we may require this later when restoring
    new = abspath_join(APP_ROOT, VERSION, 'etc')
    root_conf = abspath_join(APP_ROOT, 'etc')

    if os.path.exists(previous):
        LOG.info('Copying configuration from {0} to {1}. Overwrite existing'.format(previous, new))
        for name in os.listdir(previous):
            name_new = abspath_join(new, name)
            if os.path.exists(name_new):
                rm_r(name_new)
            cp_r(abspath_join(previous, name), name_new)

    if os.path.exists(new):
        LOG.info('Copying configuration from {0} to {1}. No overwrite.'.format(new, root_conf))
        for name in os.listdir(new):
            name_prev = abspath_join(root_conf, name)
            if not os.path.exists(name_prev):
                cp_r(abspath_join(new, name), name_prev)


def open_firewall():
    """
    Open firewall ports 8008-8014
    """
    LOG.info('Opening firewall')
    subprocess.check_call(
        'C:\\Windows\\system32\\netsh.EXE '
        'advfirewall firewall add rule name=Scalarizr '
        'dir=in protocol=tcp localport=8008-8014 action=allow',
        shell=True)


def install_winservices():
    subprocess.check_call('"{0}\\bin\\scalarizr.exe" --install-win-services'.format(CURRENT), shell=True)
    subprocess.check_call('"{0}\\bin\\scalr-upd-client.exe" --startup auto install'.format(CURRENT), shell=True)


def start_szr():
    subprocess.check_call('C:\\Windows\\System32\\net.exe start Scalarizr', shell=True)


def stop_szr():
    """
    Stop any running scalarizr services.
    """
    LOG.info('Stopping services')
    out = subprocess.check_output(
        'C:\\WINDOWS\\system32\\WindowsPowerShell\\v1.0\\Powershell.exe -Command {Get-Service}', shell=True)
    if 'Scalarizr' in out:
        subprocess.check_call('C:\\Windows\\System32\\net.exe stop Scalarizr', shell=True)


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
    bin_locations = [location for location in current_path.split(";") if 'scalarizr' not in location.lower()]
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


def cp_r(src, dst):
    """
    Copy file or dir.
    """
    try:
        shutil.copytree(src, dst)
    except OSError as exc:  # python >2.5
        if exc.errno == errno.ENOTDIR:
            shutil.copy(src, dst)
        else:
            raise


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
    Recursively remove given path.
    """
    try:
        os.remove(path)
    except OSError:
        try:
            os.rmdir(path)
        except (OSError, WindowsError):
            shutil.rmtree(path)


if __name__ == '__main__':
    main()
