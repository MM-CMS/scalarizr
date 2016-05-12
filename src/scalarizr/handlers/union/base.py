import binascii
import copy
import functools
import json
import logging
import os
import re
import sys
import threading
import time
import uuid

from agent.tasks import execute, service, chef
from agent.utils import taskutil
from common.utils import exc, sysutil
from common.utils.facts import fact
from scalarizr import handlers
from scalarizr.messaging import Messages, Queues
from scalarizr.node import __node__
import common.types


LOG = logging.getLogger(__name__)
__node__['scripting_log_dir'] = \
    os.path.join(__node__['log_dir'], 'scripting') \
    if fact['os']['family'] == 'windows' \
    else os.path.join(__node__['log_dir'], 'scalarizr', 'scripting')

def get_handlers():
    return [BaseBehaviorHandler()]

class BaseBehaviorHandler(handlers.Handler):
    def __init__(self):
        __node__['events'].on(start=self.on_start)

    def accept(self, message, queue, **kwds):
        return message.name not in (
            Messages.INT_SERVER_HALT,
            Messages.INT_SERVER_REBOOT)

    def __call__(self, message):
        if not __node__['base']['union_script_executor']:
            return
        evts = __node__['events']
        if 'scripts' in message.body:
            execute_scripts(message)
        if message.name == Messages.HOST_INIT_RESPONSE:
            if 'chef' in message.body:
                evts.on(before_host_up=functools.partial(setup_chef_automation, message))
            if 'host_init_response' not in evts._listeners:
                evts.on(host_init_response=install_behaviors)
            else:
                evts._listeners['host_init_response'].insert(0, install_behaviors)
            evts.on(host_init_response=handlers.check_supported_behaviors)



    def on_start(self, *args, **kwds):
        __node__['periodical_executor'].add_task(
            rotate_task_dirs, 3600,
            title='Rotate tasks data')
        __node__['periodical_executor'].add_task(
            rotate_scripting_logs, 3600,
            title='Rotate scripting logs')
        farm_role_params = __node__['queryenv'].list_farm_role_params(__node__['farm_role_id'])
        chef_data = farm_role_params['params'].get('chef')
        if chef_data and int(chef_data.get('daemonize', False)):
            try:
                service.start('chef-client')
            except:
                msg = "Can't daemonize Chef: {}".format(sys.exc_info()[1])
                LOG.warn(msg, exc_info=sys.exc_info())


def get_env(message):
    env = os.environ.copy()
    gv = dict(
        (kv['name'], kv['value'].encode('utf-8') if kv['value'] else '')
        for kv in message.body.get('global_variables') or [])
    if fact['os']['family'] == 'windows':
        gv = dict(
            (k.encode('ascii'), v.encode('ascii'))
            for k, v in gv.items())
    env.update(gv)
    return env

_matches_url = re.compile(r'^http(s)?://').search
def execute_scripts(message, bollard_before=None, reraise=False):
    kwds_ = {
        'env': get_env(message),
        'return_stdout': True,
        'return_stderr': True}

    for script in message.body.get('scripts') or []:
        kwds = kwds_.copy()
        if 'chef' in script:
            # Chef execute
            task, chef_data = prepare_chef_task(script['chef'])
            kwds.update(chef_data)
        else:
            # Script execute
            task = 'execute.script'
            kwds['action_type'] = 'script'
            if _matches_url(script.get('path', '')):
                kwds['url'] = script['path']
            elif script.get('path'):
                kwds['path'] = script['path']
            else:
                kwds['code'] = script['body']
                kwds['name'] = script['name']
        kwds['timeout'] = int(script['timeout'])
        kwds['run_as'] = script.get('run_as')

        event_name = message.event_name \
            if message.name == Messages.EXEC_SCRIPT \
            else message.name
        push_task = functools.partial(
            push_execute, event_name, message.body, script)
        if bollard_before:
            bollard_before(task, kwds)
        result = __node__['bollard'].apply_async(
            task,
            kwds={'action': kwds},
            callbacks={
                'task.push': push_task,
                'task.after': symlink_execute_logs,
                'task.pull': pull_execute})
        try:
            result.get()
        except:
            if reraise or fail_init_maybe.yes:
                fail_init_maybe.yes = False
                raise


def push_execute(event_name, message_body, script, task, meta):
    meta['persistent'].update({
        'script_name': script['name'],
        'script_path': script.get('path'),
        'run_as': script.get('run_as'),
        'event_name': event_name,
        #'role_name': message_body.get('role_name', 'unknown_role'),
        'event_server_id': message_body.get('server_id'),
        'event_id': message_body.get('event_id'),
        'execution_id': script['execution_id']})


def pull_execute(task, meta):
    send_exec_script_result(task, meta)
    fail_init_maybe(task, meta)


def send_exec_script_result(task, meta):
    if task['state'] == 'failed':
        if isinstance(task.exception, execute.ExecuteError):
            data = task.exception.__dict__
        else:
            data = {
                'stdout': '',
                'stderr': str(task.exception),
                'return_code': 1}
    elif task.name == 'chef.bootstrap':
        data = task.result.executed_script.to_primitive()
    else:
        data = task.result.to_primitive()
    data['stdout'] = binascii.b2a_base64(data['stdout'])
    data['stderr'] = binascii.b2a_base64(data['stderr'])
    data.pop('interpreter', None)
    data.pop('path', None)
    data.pop('command', None)
    data.pop('success_codes', None)
    data.pop('stdout_total_bytes', None)
    data.pop('stderr_total_bytes', None)
    data.update(meta.get('persistent', {}))
    __node__['messaging'].send('ExecScriptResult', body=data, queue=Queues.LOG)

def symlink_execute_logs(task, meta):
    if meta['persistent']['execution_id']:
        name_prefix = '{script_name}.{event_name}.{execution_id}'.format(
            **meta['persistent'])
    else:
        name_prefix = '{script_name}.{event_name}.{role_name}.{}'.format(
            task.id, **meta['persistent'])

    if task['state'] == 'completed' or isinstance(task.exception, execute.ExecuteError):
        if task['state'] == 'completed':
            result = task.result['executed_script'] \
                if task.name == 'chef.bootstrap' \
                else task.result
        else:
            result = task.exception.__dict__
        # Symlink log files to old location
        sysutil.mkdir_p(__node__['scripting_log_dir'])
        for name in ('out', 'err'):
            os.symlink(
                result['std{}_log_path'.format(name)],
                os.path.join(
                    __node__['scripting_log_dir'],
                    '{}-{}.log'.format(name_prefix, name)))
    else:
        # Scalr will query for logs despeate of the fact that script wasn't executed.
        # We should make fake logs from exception message.
        log_file = os.path.join(__node__['scripting_log_dir'], '{}-err.log'.format(name_prefix))
        with open(log_file, 'w+') as fp:
            fp.write(str(task.exception))
        log_file = os.path.join(__node__['scripting_log_dir'], '{}-out.log'.format(name_prefix))
        open(log_file, 'w+').close()


def fail_init_maybe(task, meta):
    if task['state'] == 'failed' and \
        __node__['state'] == 'initializing' and \
        __node__['base']['abort_init_on_script_fail'] and \
        meta['persistent']['event_server_id'] == __node__['server_id'] and \
        meta['persistent']['event_name'] == 'BeforeHostUp':
        fail_init_maybe.yes = True
fail_init_maybe.yes = False


def rotate_task_dirs():
    min_ctime = time.time() - __node__['base']['keep_scripting_logs_time']
    tasks_dir = taskutil.AgentTaskExtension.TASKS_DIR
    if not os.path.exists(tasks_dir):
        return
    for name in os.listdir(tasks_dir):
        path = os.path.join(tasks_dir, name)
        if os.stat(path).st_ctime < min_ctime:
            LOG.debug('Remove {}'.format(path))
            sysutil.rm_rf(path)


def rotate_scripting_logs():
    if not os.path.exists(__node__['scripting_log_dir']):
        return
    for name in os.listdir(__node__['scripting_log_dir']):
        path = os.path.join(__node__['scripting_log_dir'], name)
        if os.path.islink(path) and not os.path.exists(os.path.realpath(path)):
            LOG.debug('Remove {}'.format(path))
            os.unlink(path)


def setup_chef_automation(hir, hostup, *args):
    data = copy.deepcopy(hir.body['chef'])
    # execute chef bootstrap script
    hi_chef = lambda: None
    hi_chef.name = data.get('event_name') or 'HostInit'
    hi_chef.role_name = hir.role_name
    hi_chef.body = {
        'server_id': __node__['server_id'],
        'global_variables': hir.body['global_variables'],
        'scripts': [{
            'name': data.get('script_name') or '[Scalr built-in] Chef bootstrap',
            'execution_id': data.get('execution_id') or str(uuid.uuid4()),
            'asynchronous': 0,
            'timeout': sys.maxint,
            # why? hostup timeout not passed,
            # but chef one should be greater then hostup gipotetic value
            'chef': data}]}
    def bollard_before(task, kwds):
        # a callback to get evaluated node_name
        if task == 'chef.bootstrap':
            data['node_name'] = kwds['node_name']
    execute_scripts(hi_chef, bollard_before, reraise=True)
    hostup.body['chef'] = data


def prepare_chef_task(chef_data):
    kwds = {}
    if chef_data.get('ssl_verify_mode', 'chef_auto') != 'chef_auto':
        kwds['ssl_verify_mode'] = chef_data.get('ssl_verify_mode')
    if chef_data.get('log_level'):
        kwds['log_level'] = chef_data['log_level']
    run_list = extract_chef_run_list(chef_data)
    kwds['json_attributes'] = extract_chef_json_attributes(chef_data)
    if 'cookbook_url' in chef_data:
        task = 'chef.solo'
        kwds['action_type'] = 'chef.solo'
        kwds['solo_rb_template'] = chef_data.get('solo_rb_template')
        kwds['json_attributes']['run_list'] = run_list
        kwds['cookbooks'] = {}
        if chef_data.get('cookbook_url_type') == 'git':
            kwds['cookbooks']['source_type'] = 'repository'
            kwds['cookbooks']['repo'] = {
                'url': chef_data['cookbook_url'],
                'ssh_key': chef_data.get('ssh_private_key')}
            if chef_data.get('relative_path'):
                kwds['cookbooks']['relative_path'] = chef_data['relative_path']
        elif chef_data.get('cookbook_url_type') == 'http':
            if chef_data['cookbook_url'].startswith('file://'):
                kwds['cookbooks']['source_type'] = 'path'
                kwds['cookbooks']['path'] = chef_data['cookbook_url'][7:]
            else:
                kwds['cookbooks']['source_type'] = 'url'
                kwds['cookbooks']['url'] = chef_data['cookbook_url']
        else:
            raise exc.MalformedError('Chef cookbook_url_type is not valid: {}'.format(
                chef_data['cookbook_url_type']))
    else:
        if chef_data.get('server_url'):
            # Bootstrap
            kwds['action_type'] = 'chef.bootstrap'
            kwds['client_rb_template'] = chef_data.get('client_rb_template')
            kwds['json_attributes']['run_list'] = run_list
            kwds['server_url'] = chef_data['server_url']
            kwds['validation_key'] = chef_data['validator_key']
            kwds['validation_client_name'] = chef_data['validator_name']
            if chef_data.get('environment'):
                kwds['environment'] = chef_data['environment']
            kwds['node_name'] = chef_data.get('node_name') or default_chef_node_name()
            kwds['daemonize'] = bool(int(chef_data.get('daemonize', 0)))
        elif run_list:
            # Override run_list
            kwds['action_type'] = 'chef.override'
            kwds['run_list'] = run_list
        else:
            # Re-converge
            kwds['action_type'] = 'chef.reconverge'

    return kwds['action_type'], kwds

'''
  # Chef-Solo
  - asynchronous: '0'
    chef:
      cookbook_url: https://github.com/Scalr/cookbooks.git
      cookbook_url_type: git
      json_attributes: '{"dummy": "what?", "nested": ["o", "u", "yeah"]}'
      relative_path: ./cookbooks
      run_list: '["recipe[apt]"]'
    execution_id: 1f50d1de-2261-43ce-a874-2089e92dd8ac
    name: chef-0336
    timeout: '1200'

  # Override run_list
  scripts:
  - asynchronous: '0'
    chef:
      json_attributes: '{"dummy": "what?", "nested": ["no", "o", "o"]}'
      run_list: '["role[dummy_role]"]'
    execution_id: 79f82164-b88d-4740-8cda-abcb8e8504b6
    name: chef-0234
    timeout: '1200'

  # Re-converge
  - asynchronous: '0'
    chef:
      json_attributes: ''
      run_list: ''
    execution_id: a5891b00-9e5a-4a31-a2e6-a54bb91fed62
    name: chef-0597
    timeout: '1200'
'''

def default_chef_node_name():
    hostname = __node__['base'].get('hostname')
    if hostname:
        return re.sub('\s+', '-', hostname)
    else:
        return '{0}-{1}-{2}'.format(
            __node__['platform'].name,
            __node__['platform'].get_public_ip(),
            time.time())

def extract_chef_run_list(chef_data):
    if chef_data.get('run_list'):
        try:
            return json.loads(chef_data['run_list'])
        except ValueError as e:
            raise exc.MalformedError("Chef run list is not a valid JSON: {0}".format(e))
    elif chef_data.get('role'):
        return ['role[{}]'.format(chef_data['role'])]

def extract_chef_json_attributes(chef_data):
    """
    Extract json attributes dictionary from scalr formatted structure
    """
    try:
        return json.loads(chef_data.get('json_attributes') or "{}")
    except ValueError as e:
        raise exc.MalformedError("Chef attributes is not a valid JSON: {0}".format(e))


# Role-builer uses the same mapping
BEHAVIORS_RECIPES_MAP = dict(
    apache="recipe[apache2]",
    haproxy="recipe[haproxy]",
    mariadb="recipe[mariadb]",
    memcached="recipe[memcached]",
    mysql2="recipe[mysql::server]",
    mysql="recipe[mysql::server]",
    nginx="recipe[nginx]",
    percona="recipe[percona]",
    rabbitmq="recipe[rabbitmq]",
    redis="recipe[redis]",
    tomcat="recipe[tomcat]"
)
BEHAVIORS_RECIPES_MAP['www'] = BEHAVIORS_RECIPES_MAP['nginx']
BEHAVIORS_RECIPES_MAP['app'] = BEHAVIORS_RECIPES_MAP['apache']

SCALR_COOKBOOKS_GIT_URL = "git://github.com/Scalr/cookbooks.git"


def install_behaviors(hir_message):
    behaviors = hir_message.body.get('base', {}).get('install', {}).get('behaviors', [])
    if not behaviors:
        return

    unknown_bhs = set(behaviors).difference(set(BEHAVIORS_RECIPES_MAP.keys()))
    if unknown_bhs:
        raise Exception('Unknown behaviors: {}'.format(list(unknown_bhs)))

    LOG.info('Installing software for behaviors: {}'.format(behaviors))
    chef_action = {
        'action_type': 'chef.solo',
        'json_attributes': {
            'run_list': [BEHAVIORS_RECIPES_MAP[b] for b in behaviors]
            },
        'cookbooks': {
            'source_type': 'repository',
            'repo': {
                'url': SCALR_COOKBOOKS_GIT_URL
                },
            'relative_path': 'cookbooks'
            }
        }
    task = __node__['bollard'].apply_async
    task('chef.install', kwds={'version': 'auto'}).get()
    task('pkgmgr.updatedb').get()
    task('chef.solo', kwds={'action': chef_action}).get()
