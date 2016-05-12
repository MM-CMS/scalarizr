import logging

from scalarizr.messaging import MessageService
from scalarizr.messaging import Queues
from scalarizr.messaging.p2p.security import P2pMessageSecurity
from scalarizr.messaging.p2p import producer
from scalarizr.messaging.p2p import consumer
from scalarizr.messaging.p2p.store import P2pMessage


LOG = logging.getLogger(__name__)


class P2pMessageService(MessageService):
    _params = {}
    _default_producer = None
    _default_consumer = None

    def __init__(self, **params):
        self._params = params
        self._security = P2pMessageSecurity(params["server_id"], params["crypto_key_path"])

    def new_message(self, name=None, meta=None, body=None):
        return P2pMessage(name, meta, body)

    def get_consumer(self):
        if not self._default_consumer:
            self._default_consumer = self.new_consumer(
                    endpoint=self._params["consumer_url"],
                    msg_handler_enabled=self._params.get("msg_handler_enabled", True))
        return self._default_consumer

    def new_consumer(self, **params):
        c = consumer.P2pMessageConsumer(**params)
        c.filters['protocol'].append(self._security.in_protocol_filter)
        return c

    def get_producer(self):
        if not self._default_producer:
            self._default_producer = self.new_producer(
                    endpoint=self._params["producer_url"],
                    retries_progression=self._params.get("producer_retries_progression"))
        return self._default_producer

    def new_producer(self, **params):
        p = producer.P2pMessageProducer(**params)
        p.filters['protocol'].append(self._security.out_protocol_filter)
        return p

    def send(self, name, body=None, meta=None, queue=None):
        msg = self.new_message(name, meta, body)
        self.get_producer().send(queue or Queues.CONTROL, msg)


def new_service(**kwargs):
    return P2pMessageService(**kwargs)
