'''
Created on Jul 23, 2010

@author: marat
@author: Dmytro Korsakov
'''

import logging

from scalarizr.api import memcached as memcached_api
from scalarizr.bus import bus
from scalarizr.config import BuiltinBehaviours
from scalarizr.handlers import Handler, FarmSecurityMixin
from scalarizr.messaging import Messages
from scalarizr.node import __node__


BEHAVIOUR = SERVICE_NAME = BuiltinBehaviours.MEMCACHED


def get_handlers():
    return [MemcachedHandler()]


class MemcachedHandler(Handler, FarmSecurityMixin):

    _logger = None
    _queryenv = None
    _ip_tables = None
    _port = None
    _api = None

    def __init__(self):
        Handler.__init__(self) # init script will set later
        FarmSecurityMixin.__init__(self)
        self.init_farm_security([11211])
        self._logger = logging.getLogger(__name__)
        self._queryenv = bus.queryenv_service
        bus.on(init=self.on_init, start=self.on_start)

    def accept(self, message, queue, behaviour=None, platform=None, os=None, dist=None):
        return message.name in \
            (Messages.HOST_INIT, \
            Messages.HOST_DOWN) \
            and BEHAVIOUR in behaviour

    def _defer_init(self):
        self._api = memcached_api.MemcachedAPI()

    def on_init(self):
        bus.on(before_host_up=self.on_before_host_up)

    def on_start(self):
        if __node__['state'] == 'running':
            self._defer_init()
            self._api.start_service()

    def on_before_host_up(self, message):
        self._defer_init()
        self._api.start_service()
