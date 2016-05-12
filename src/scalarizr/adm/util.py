import prettytable
import sys
import os

from scalarizr.node import __node__
from scalarizr.messaging.p2p import service as p2p_service


def make_table(data_rows, header=None):
    """Returns PrettyTable object applicable to print"""

    def normalize_multiline(x):
        if sys.version_info[0:2] < (2, 7) and type(x) == str:
            return x.replace('\n', '')
        return x

    if not data_rows:
        data_rows = [[]]
    max_row_length = len(header) if header else max(map(len, data_rows))
    table = prettytable.PrettyTable(header if header else range(max_row_length))
    table.header = bool(header)

    for row in data_rows:
        if not row:
            row = []
        row = [normalize_multiline(x) for x in row]
        row_length = len(row)
        if row_length > 0:
            if row_length != max_row_length:
                row = (row + [None]*max_row_length)[:max_row_length]
            table.add_row(row)

    return table


def encode(obj, encoding='ascii'):
    if isinstance(obj, basestring):
        try:
            return obj.encode(encoding)
        except UnicodeEncodeError:
            return obj
    elif isinstance(obj, list):
        return [encode(item) for item in obj]
    elif isinstance(obj, dict):
        return dict((encode(k), encode(v)) for k, v in obj.items())
    else:
        return obj


def new_messaging_service():
    return p2p_service.P2pMessageService(
        server_id=__node__['server_id'],
        crypto_key_path=os.path.join(__node__['etc_dir'], __node__['crypto_key_path']),
        producer_url=__node__['producer_url'])
