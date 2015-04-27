import csv
from datetime import datetime, timedelta
import io
import json
import logging
import os

from .api import GOVUK, PerformancePlatform
from .data import Datapoint, AggregatedDatasetCombiningSmartAnswers
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


class InfoStatistics(object):
    """
    Generate the aggregated data for the PP's info-statistics dataset.

    This is used to identify pages with high numbers of problem reports and
    searches.
    It does the following:
    - Fetches data from the PP for all pages with problem reports
      or searches
    - Aggregates problem reports for smart answers to the level of
      the starting URL, to aid comparison
    - Initialises a neat output dataset
    - For all URLs with problem reports or searches, fetches data
      on the number of unique page views
    - Normalise problem reports / searches by the number of unique
      page views
    - Write output to a local CSV file and to the PP
    """

    def __init__(self, pp_token, start_date=None, end_date=None):
        """
        Start and end dates are assumed to be UTC. They can be dates or datetimes.
        """
        self.end_date = end_date or datetime.utcnow()
        self.start_date = start_date or (self.end_date - timedelta(days=settings.DAYS))
        self.pp_adapter = PerformancePlatform(pp_token, self.start_date, self.end_date)
        self.csv_writer = CSVWriter(start_date=self.start_date, end_date=self.end_date)

    def process_data(self):
        smart_answers = GOVUK().get_smart_answers()
        dataset = self._load_performance_data(smart_answers)

        aggregated_datapoints = dataset.get_aggregated_datapoints().values()

        self.csv_writer.write_datapoints(aggregated_datapoints)
        self.pp_adapter.save_aggregated_results(aggregated_datapoints)

    def _load_performance_data(self, smart_answers):
        logger.info('Loading performance data')

        dataset = AggregatedDatasetCombiningSmartAnswers(smart_answers)
        problem_report_counts = self.pp_adapter.get_problem_report_counts()
        search_counts = self.pp_adapter.get_search_counts()
        involved_paths = list(set(problem_report_counts.keys() + search_counts.keys()))
        involved_paths.sort()

        logger.info('Found %d paths to get pageview counts for', len(involved_paths))
        for path in involved_paths:
            logger.debug(path)

        unique_pageviews = self.pp_adapter.get_unique_pageviews(involved_paths)

        dataset.add_unique_pageviews(unique_pageviews)
        dataset.add_problem_report_counts(problem_report_counts)
        dataset.add_search_counts(search_counts)

        return dataset
