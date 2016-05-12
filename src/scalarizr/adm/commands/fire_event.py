from scalarizr.adm.command import Command
from scalarizr.node import __node__
from scalarizr.adm.util import new_messaging_service


class FireEvent(Command):
    """
    Fires event with given name and parameters on Scalr. Parameters should be
    passed in <key>=<value> form.

    Usage:
      fire-event <name> [<kv>...]
    """

    def __call__(self, name=None, kv=None):
        if not kv:
            kv = {}
        msg_service = new_messaging_service()
        producer = msg_service.get_producer()

        producer.endpoint = __node__['producer_url']

        params = {}
        for pair in kv:
            if '=' in pair:
                k, v = pair.split('=')
                params[k] = v

        message_name = 'FireEvent'
        body = {'event_name': name, 'params': params}
        if name == 'InitFailed':
            body = params
            message_name = name
        msg = msg_service.new_message(message_name, body=body)
        print('Sending %s' % name)
        producer.send('control', msg)

        print("Done")


commands = [FireEvent]
