import csv
import logging

from .data import Datapoint
import settings


logger = logging.getLogger(__name__)


class CSVWriter(object):
    """
    Write datapoints to a CSV file.

    The filename can be passed in, or a date-based one will be used.
    """
    def __init__(self, start_date=None, end_date=None, output_filename=None):
        if output_filename is None and None in (start_date, end_date):
            raise ValueError('CSVWriter requires either output_filename or both start_date and end_date')

        self.output_filename = output_filename or self._csv_filename(start_date, end_date)

    @staticmethod
    def _format_date(date_or_datetime):
        return date_or_datetime.strftime('%Y-%m-%d')

    def _csv_filename(self, start_date, end_date):
        return settings.REPORT_FILENAME.format(self._format_date(start_date),
                                               self._format_date(end_date))

    def write_datapoints(self, datapoints):
        with open(self.output_filename, 'w') as report:
            fieldnames = Datapoint.all_fields
            writer = csv.DictWriter(report, fieldnames=fieldnames)

            logger.info('Writing report to CSV file: %s', self.output_filename)

            writer.writeheader()
            writer.writerows(dp.as_dict() for dp in datapoints)
