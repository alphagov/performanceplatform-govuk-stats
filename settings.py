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


logger = logging.getLogger('stats')
logger.setLevel(logging.INFO)

handler = logging.StreamHandler(stream=sys.stdout)
handler.setLevel(logging.INFO)

logger.addHandler(handler)
