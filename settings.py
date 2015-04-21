import logging
import os
import sys


DATA_DOMAIN = os.environ.get(
    'PP_DATA_DOMAIN',
    'https://www.performance.service.gov.uk/data'
)
PP_TOKEN = os.environ.get('PP_DATASET_TOKEN', None)

DATA_GROUP = 'govuk-info'
DAYS = 42
RESULTS_DATASET = 'info-statistics'


LOG_LEVEL = os.environ.get('LOG_LEVEL', 'INFO')
logging_level = getattr(logging, LOG_LEVEL.upper())

logger = logging.getLogger('stats')
logger.setLevel(logging_level)

handler = logging.StreamHandler(stream=sys.stdout)
handler.setLevel(logging_level)

formatter = logging.Formatter(fmt='%(asctime)s %(message)s')
handler.setFormatter(formatter)

logger.addHandler(handler)
