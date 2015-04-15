import os


DATA_DOMAIN = os.environ.get(
    'PP_DATA_DOMAIN',
    'https://www.performance.service.gov.uk/data'
)
PP_TOKEN = os.environ.get('PP_DATASET_TOKEN', None)

DATA_GROUP = 'govuk-info'
DAYS = 42
RESULTS_DATASET = 'info-statistics'
