"""Scribe Writer Class

This class handles writing to a scribe instance. The difference
between this class and logger is that this allows you to stream raw
data in your own format.

*Usage*
>>> from scribe_logger.writer import ScribeWriter
>>> writer = ScribeWriter('localhost', 1463, "category")
>>> writer.write("my message")
>>> writer.write("my message", 'another_category')

"""
from scribe import scribe
from thrift.transport import TTransport, TSocket
from thrift.protocol import TBinaryProtocol
from thrift import Thrift
import time
import threading


class ScribeWriter(object):

    def __init__(self, host, port, default_category='default'):
        self.default_category = default_category
        self._configure_scribe(host, port)
        self.lock = threading.RLock()

    def write(self, category, data):
        """Write data to scribe instance"""

        if not self._is_scribe_ready():
            return

        if not isinstance(data, list):
            data = [data]

        category = category or self.default_category
        messages = []
        
        for msg in data:
            try:
                entry = scribe.LogEntry(category=category, message=msg)
            except Exception, e:
                entry = scribe.LogEntry(dict(category=category, message=msg))
                
            messages.append(entry)

        self.lock.acquire()
        try:
            self.client.Log(messages=messages)
        except Thrift.TException, tx:
            self.transport.close()
        except Exception, e:
            self.transport.close()
        finally:
            self.lock.release()

    def _configure_scribe(self, host, port):
        self.socket = TSocket.TSocket(host=host, port=port)
        self.socket.setTimeout(1000)
        self.transport = TTransport.TFramedTransport(self.socket)
        self.protocol = TBinaryProtocol.TBinaryProtocolAccelerated(
            trans=self.transport, strictRead=False, strictWrite=False)
        self.client = scribe.Client(iprot=self.protocol, oprot=self.protocol)

    def _is_scribe_ready(self):
        """Check to see if scribe is ready to be written to"""
        if self.transport.isOpen():
            return True

        self.lock.acquire()
        try:
            self.transport.open()
            return True
        except Thrift.TException, tx:
            self.transport.close()
        except Exception, e:
            self.transport.close()
        finally:
            self.lock.release()
        return False