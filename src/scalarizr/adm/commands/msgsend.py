from scalarizr.adm.command import Command
from scalarizr.node import __node__
from scalarizr.adm.util import new_messaging_service


class Msgsnd(Command):
    """
    Sends message with given name using given endpoint throughout given queue.
    Parameters are passed in <key>=<value> form or can be loaded from msgfile.

    Usage:
      msgsnd [--queue=<queue>] [--name=<name>] [--msgfile=<msgfile>] [--endpoint=<endpoint>] [<kv>...]

    Options:
      -n <name>, --name=<name>
      -f <msgfile>, --msgfile=<msgfile>
      -e <endpoint>, --endpoint=<endpoint>
      -o <queue>, --queue=<queue>
    """

    def __call__(self, name=None, msgfile=None, endpoint=None, queue=None, kv=None):
        if not msgfile and not name:
            raise Exception('msgfile or name sholuld be presented')
        if not kv:
            kv = {}

        msg_service = new_messaging_service()
        msg = msg_service.new_message()

        if msgfile:
            data = None
            with open(msgfile, 'r') as fp:
                data = fp.read()
            if data:
                for method in (msg.fromxml, msg.fromjson):
                    try:
                        method(data)
                        break
                    except:
                        pass
                else:
                    raise Exception('Unknown message format')
        else:
            body = {}
            for pair in kv:
                if '=' in pair:
                    k, v = pair.split('=')
                    body[k] = v
            msg.body = body
        if name:
            msg.name = name
        producer = msg_service.get_producer()
        producer.endpoint = endpoint or __node__['producer_url']
        producer.send(queue, msg)

        print("Done")


commands = [Msgsnd]
