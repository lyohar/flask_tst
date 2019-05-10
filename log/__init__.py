import logging, logging.config
import os

logging.config.fileConfig('{0}/logger.ini'.format(os.path.dirname(__file__)))

logger = logging.getLogger("ods2")