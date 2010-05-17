"""Scribe Logger Class

This class handles overwriting logging to send to a scribe instance.

"""
from scribe_logger.writer import ScribeWriter
import logging
import logging.handlers
import sys


class ScribeLogHandler(logging.Handler, ScribeWriter):
    """"""
    def __init__(self, category=None, extra=None, host='127.0.0.1', port=1463):
        logging.Handler.__init__(self)

        if category:
            self.category = category

        if extra:
            self.extra = ' '.join(extra)
        else:
            self.extra = ''

        ScribeWriter.__init__(self, host, port, self.category)

    def set_category(self, category):
        self._category = category

    def get_category(self):
        return getattr(self, '_category', 'default')

    category = property(get_category, set_category)

    def emit(self, record):
        record = "%s %s" % (self.extra, self.format(record))

        try:
            self.write(self.category, record)
        except (KeyboardInterrupt, SystemExit):
            raise
        except:
            self.handleError(record)

    def flush(self):
        pass