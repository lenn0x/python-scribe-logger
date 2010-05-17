from scribe_logger.logger import ScribeLogHandler
import logging

my_logger = logging.getLogger('MyLogger')
my_logger.setLevel(logging.DEBUG)

scribe = ScribeLogHandler(category="test_category")
scribe.setLevel(logging.DEBUG)
my_logger.addHandler(scribe)

my_logger.info("This is a test message")
