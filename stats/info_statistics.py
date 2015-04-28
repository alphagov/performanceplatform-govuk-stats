from datetime import datetime, timedelta
import logging

from .api import GOVUK, PerformancePlatform
from .csv_writer import CSVWriter
from .data import Datapoint, AggregatedDatasetCombiningSmartAnswers
import settings


logger = logging.getLogger(__name__)


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
