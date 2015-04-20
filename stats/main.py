import os
import sys

from stats import info_statistics
import settings


if not settings.PP_TOKEN:
    msg = 'You need to set the dataset token for the PP '
    msg += '{0}/{1} '.format(settings.DATA_GROUP, settings.RESULTS_DATASET)
    msg += 'dataset to run this script. You can get this from '
    msg += 'https://stagecraft.production.performance.service.gov.uk/admin/'
    sys.exit(msg)
else:
    c = info_statistics.InfoStatistics(settings.PP_TOKEN)
    c.process_data()
