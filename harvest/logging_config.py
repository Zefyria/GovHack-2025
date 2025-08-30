# logging_config.py
import logging

logging.basicConfig(
    filename='harvest.log',
    level=logging.INFO,
    format='%(asctime)s %(levelname)s:%(message)s'
)
logger = logging.getLogger()
