import logging
import sys
import threading
import time

from scalarizr.bus import bus
from scalarizr.messaging import Message, MessagingError
from scalarizr.node import __node__


class _P2pMessageStore(object):
    _logger = None

    TAIL_LENGTH = 50

    def __init__(self):
        self._logger = logging.getLogger(__name__)
        self._local_storage_lock = threading.Lock()
        self._unhandled = None
        self._received = []
        ex = bus.periodical_executor
        if ex:
            self._logger.debug('Add rotate messages table task for periodical executor')
            ex.add_task(self.rotate, 3600, 'Rotate messages sqlite table')

    def _conn(self):
        return bus.db

    @property
    def _unhandled_messages(self):
        if self._unhandled is None:
            self._unhandled = self._get_unhandled_from_db()
        return self._unhandled

    def rotate(self):
        conn = self._conn()
        cur = conn.cursor()
        cur.execute('SELECT * FROM p2p_message ORDER BY id DESC LIMIT %d, 1' % self.TAIL_LENGTH)
        row = cur.fetchone()
        if row:
            self._logger.debug('Deleting messages older then messageid: %s', row['message_id'])
            cur.execute('DELETE FROM p2p_message WHERE id <= ?', (row['id'],))
        conn.commit()

    def put_ingoing(self, message, queue, consumer_id):
        with self._local_storage_lock:
            if message.id in self._received:
                self._logger.debug('Ignore message {!r} (already received)'.format(message.id))
                return
            self._received.append(message.id)
            if len(self._received) > self.TAIL_LENGTH:
                self._received = self._received[-self.TAIL_LENGTH:]

            self._unhandled_messages.append((queue, message))

            conn = self._conn()
            cur = conn.cursor()
            try:
                sql = 'INSERT INTO p2p_message (id, message, message_id, ' \
                        'message_name, queue, is_ingoing, in_is_handled, in_consumer_id, format) ' \
                        'VALUES (NULL, ?, ?, ?, ?, ?, ?, ?, ?)'

                #self._logger.debug('Representation mes: %s', repr(str(message)))
                cur.execute(sql, [message.tojson().decode('utf-8'), message.id,
                    message.name, queue, 1, 0, consumer_id, 'json'])
                if message.meta.has_key('request_id'):
                    cur.execute("""UPDATE p2p_message
                                    SET response_uuid = ? WHERE message_id = ?""",
                            [message.id, message.meta['request_id']])

                self._logger.debug("Commiting put_ingoing")
                conn.commit()
                self._logger.debug("Commited put_ingoing")
            finally:
                cur.close()

    def get_unhandled(self, consumer_id):
        with self._local_storage_lock:
            ret = []
            for queue, message in self._unhandled_messages:
                msg_copy = P2pMessage()
                msg_copy.fromjson(message.tojson())
                ret.append((queue, msg_copy))

            return ret

    def _get_unhandled_from_db(self):
        """
        Return list of unhandled messages in obtaining order
        @return: [(queue, message), ...]
        """
        cur = self._conn().cursor()
        try:
            sql = 'SELECT queue, message_id FROM p2p_message ' \
                'WHERE is_ingoing = ? AND in_is_handled = ? ' \
                'ORDER BY id'
            cur.execute(sql, [1, 0])

            ret = []
            for r in cur.fetchall():
                ret.append((r["queue"], self.load(r["message_id"], True)))
            return ret
        finally:
            cur.close()

    def mark_as_handled(self, message_id):
        with self._local_storage_lock:
            filter_fn = lambda x: x[1].id != message_id
            self._unhandled = filter(filter_fn, self._unhandled_messages)

        for _ in xrange(0, 5):
            try:
                msg = self.load(message_id, True)
                break
            except:
                self._logger.debug('Failed to load message %s', message_id, exc_info=sys.exc_info())
                time.sleep(1)
        else:
            self._logger.debug("Cant load message in several attempts,"
                " assume it doesn't exists. Leaving")
            return

        if 'platform_access_data' in msg.body:
            del msg.body['platform_access_data']
        msg_s = msg.tojson().decode('utf-8')

        conn = self._conn()
        cur = conn.cursor()
        try:
            sql = 'UPDATE p2p_message SET in_is_handled = ?, message = ?, ' \
                'out_last_attempt_time = datetime("now") ' \
                'WHERE message_id = ? AND is_ingoing = ?'
            cur.execute(sql, [1, msg_s, message_id, 1])
            conn.commit()
        finally:
            cur.close()

    def put_outgoing(self, message, queue, sender):
        conn = self._conn()
        cur = conn.cursor()
        try:
            sql = 'INSERT INTO p2p_message (id, message, message_id, message_name, queue, ' \
                'is_ingoing, out_is_delivered, out_delivery_attempts, out_sender, format) ' \
                'VALUES ' \
                '(NULL, ?, ?, ?, ?, ?, ?, ?, ?, ?)'

            cur.execute(sql, [message.tojson().decode('utf-8'), message.id,
                                              message.name, queue, 0, 0, 0, sender, 'json'])
            conn.commit()
        finally:
            cur.close()

    def get_undelivered(self, sender):
        """
        Return list of undelivered messages in outgoing order
        """
        cur = self._conn().cursor()
        try:
            sql = 'SELECT queue, message_id FROM p2p_message ' \
                    'WHERE is_ingoing = ? AND out_is_delivered = ? AND out_sender = ? ORDER BY id'
            cur.execute(sql, [0, 0, sender])
            ret = []
            for r in cur.fetchall():
                ret.append((r[0], self.load(r[1], False)))
            return ret
        finally:
            cur.close()

    def delivered_at(self, message_name):
        """
        Return message and delivery date
        """
        cur = self._conn().cursor()
        try:
            sql = 'SELECT message_id, out_last_attempt_time FROM p2p_message ' \
                'WHERE is_ingoing = ? AND out_is_delivered = ? AND message_name = ?' \
                'ORDER BY out_last_attempt_time DESC'
            cur.execute(sql, [0, 1, message_name])
            return cur.fetchone()
        finally:
            cur.close()

    def mark_as_delivered(self, message_id):
        return self._mark_as_delivered(message_id, 1)

    def mark_as_undelivered(self, message_id):
        return self._mark_as_delivered(message_id, 0)

    def _mark_as_delivered(self, message_id, delivered):
        conn = self._conn()
        cur = conn.cursor()
        try:
            sql = 'UPDATE p2p_message SET out_delivery_attempts = out_delivery_attempts + 1, ' \
                        'out_last_attempt_time = datetime("now"), out_is_delivered = ? ' \
                    'WHERE message_id = ? AND is_ingoing = ?'
            cur.execute(sql, [int(bool(delivered)), message_id, 0])
            conn.commit()
        finally:
            cur.close()

    def load(self, message_id, is_ingoing):
        cur = self._conn().cursor()
        try:
            cur.execute('SELECT * FROM p2p_message ' \
                        'WHERE message_id = ? AND is_ingoing = ?',
                    [message_id, int(bool(is_ingoing))])
            row = cur.fetchone()
            if not row is None:
                message = P2pMessage()
                self._unmarshall(message, row)
                return message
            else:
                raise MessagingError("Cannot find message (message_id: %s)" % message_id)
        finally:
            cur.close()

    def is_handled(self, message_id):
        with self._local_storage_lock:
            filter_fn = lambda x: x[1].id == message_id
            filtered = filter(filter_fn, self._unhandled_messages)
            return not filtered

    def is_delivered(self, message_id):
        cur = self._conn().cursor()
        try:
            cur.execute('SELECT is_delivered FROM p2p_message ' \
                        'WHERE message_id = ? AND is_ingoing = ?',
                    [message_id, 0])
            return cur.fetchone()["out_is_delivered"] == 1
        finally:
            cur.close()

    def is_response_received(self, message_id):
        cur = self._conn().cursor()
        try:
            sql = 'SELECT response_id FROM p2p_message ' \
                    'WHERE message_id = ? AND is_ingoing = ?'
            cur.execute(sql, [message_id, 0])
            return cur.fetchone()["response_id"] != ""
        finally:
            cur.close()

    def get_response(self, message_id):
        cur = self._conn().cursor()
        try:
            cur.execute('SELECT response_id FROM p2p_message ' \
                        'WHERE message_id = ? AND is_ingoing = ?',
                    [message_id, 0])
            response_id = cur.fetchone()["response_id"]
            if not response_id is None:
                return self.load(response_id, True)
            return None
        finally:
            cur.close()

    def _unmarshall(self, message, row):
        if 'json' == row["format"]:
            message.fromjson(row["message"])
        else:
            message.fromxml(row["message"])


_message_store = None
def P2pMessageStore():
    global _message_store
    if _message_store is None:
        _message_store = _P2pMessageStore()
    return _message_store


class P2pMessage(Message):

    def __init__(self, name=None, meta=None, body=None):
        Message.__init__(self, name, meta, body)
        self.__dict__["_store"] = P2pMessageStore()
        self.meta['server_id'] = __node__['server_id']

    def is_handled(self):
        return self._store.is_handled(self.id)

    def is_delivered(self):
        return self._store.is_delivered(self.id)

    def is_responce_received(self):
        return self._store.is_response_received(self.id)

    def get_response(self):
        return self._store.get_response(self.id)
