import json
import os
import sys

from scalarizr.adm import command
from scalarizr.api.binding import jsonrpc_http
from scalarizr.node import __node__
from scalarizr import util


POLLING_TIMEOUT = 60
POLLING_INTERVAL = 1


class AgentUpdate(command.Command):
    """
    Updates scalarizr.

    Usage:
      agent-update [--force]

    Options:
      -f, --force  ignore update server restrictions
    """

    def __call__(self, force=False):
        sys.stdout.write('Updating Scalarizr.')
        sys.stdout.flush()
        upd_service = jsonrpc_http.HttpServiceProxy(
            'http://localhost:8008/',
            os.path.join(__node__['etc_dir'], __node__['crypto_key_path']))
        upd_service.update(force=force, async=True)
        update_started = [False]
        status = {}

        def _completed():
            sys.stdout.write('.')
            sys.stdout.flush()
            with open(os.path.join(__node__['private_dir'], 'update.status'), 'r') as fp:
                status.update(json.load(fp))
                if status['state'] in ('completed', 'error') and update_started[0]:
                    return status
                elif not update_started[0] and status['state'].startswith('in-progress'):
                    update_started[0] = True
                return False

        try:
            util.wait_until(_completed, sleep=POLLING_INTERVAL, timeout=POLLING_TIMEOUT)
        except BaseException as e:
            if 'Timeout:' in str(e):
                status['error'] = str(e)
            else:
                raise

        if status['error']:
            print '\nUpdate failed.\n{}\n'.format(status['error'])
        else:
            print '\nDone.\nInstalled: {}\n'.format(status['installed'])


commands = [AgentUpdate]
