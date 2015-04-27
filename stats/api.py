from __future__ import print_function

import copy
import itertools
import logging
import string
import sys

from performanceplatform.client import DataSet
import requests

from .data import SmartAnswer
import settings


logger = logging.getLogger(__name__)


class PerformancePlatform(object):
    """
    Handles GETting and POSTing data to and from the Performance Platform.

    This class uses the PerformancePlatform Client, which will retry
    handling GET and POST requests up to five times, if their status
    codes are 502 or 503. If they still don't succeed, the client
    raises an exception that is not handled by us.
    """

    date_format = "%Y-%m-%dT00:00:00Z"

    def __init__(self, pp_token, start_date, end_date):
        self.pp_token = pp_token
        # Format dates here so that they won't be accidentally used as
        # non-midnight datetimes elsewhere in the class:
        self.start_date = start_date.strftime(self.date_format)
        self.end_date = end_date.strftime(self.date_format)

    def get_problem_report_counts(self):
        logger.info('Getting problem report counts')
        results_by_letter = [self._get_problem_report_counts_for_paths_starting_with('/' + letter)
                             for letter in string.lowercase]
        all_results = list(itertools.chain(*results_by_letter))
        return {result["pagePath"].encode('utf-8'): result["total:sum"]
                for result in all_results}

    def get_search_counts(self):
        logger.info('Getting search counts')
        results_by_letter = [self._get_search_counts_for_paths_starting_with('/' + letter)
                             for letter in string.lowercase]
        all_results = list(itertools.chain(*results_by_letter))
        return {result["pagePath"].encode('utf-8'): result["searchUniques:sum"]
                for result in all_results}

    def get_unique_pageviews(self, paths):
        logger.info('Getting pageview counts')
        return {path: self.get_unique_pageviews_for_path(path) for path in paths}

    def get_unique_pageviews_for_path(self, path):
        data = self._get_pp_data('page-statistics', 'uniquePageviews:sum',
                                 filter_by=path)
        if data and data[0]['uniquePageviews:sum']:
            return int(data[0]['uniquePageviews:sum'])

    def save_aggregated_results(self, results):
        data_set = DataSet.from_group_and_type(settings.DATA_DOMAIN,
                                               settings.DATA_GROUP,
                                               settings.RESULTS_DATASET,
                                               token=self.pp_token)
        enriched_results = [self._enrich_mandatory_pp_fields(result)
                            for result in results]
        logger.info('Posting data to Performance Platform')
        data_set.post(enriched_results)

    def _get_problem_report_counts_for_paths_starting_with(self, path_prefix):
        return self._get_pp_data('page-contacts', 'total:sum',
                                 filter_by_prefix=path_prefix)

    def _get_search_counts_for_paths_starting_with(self, path_prefix):
        return self._get_pp_data('search-terms', 'searchUniques:sum',
                                 filter_by_prefix=path_prefix)

    def _enrich_mandatory_pp_fields(self, result):
        enriched_result = copy.copy(result.as_dict())
        enriched_result['_timestamp'] = self.end_date
        enriched_result['_start_at'] = self.start_date
        enriched_result['_end_at'] = self.end_date
        return enriched_result

    def _get_pp_data(self, dataset_name, value,
                     filter_by=None, filter_by_prefix=None):
        dataset = DataSet.from_group_and_type(settings.DATA_DOMAIN,
                                              settings.DATA_GROUP,
                                              dataset_name)
        query_parameters = {
            'group_by': 'pagePath',
            'period': 'day',
            'start_at': self.start_date,
            'end_at': self.end_date,
            'collect': value,
        }
        if filter_by:
            query_parameters['filter_by'] = 'pagePath:' + filter_by
        elif filter_by_prefix:
            query_parameters['filter_by_prefix'] = 'pagePath:' + filter_by_prefix

        logger.debug('Getting {0} data with params {1}'.format(dataset_name, query_parameters))
        json_data = dataset.get(query_parameters)

        if 'data' in json_data:
            return json_data['data']
        else:
            return []


class GOVUK(object):

    def get_smart_answers(self):
        """Get all smart answers, from the Search API."""
        logger.info('Getting smart answers')

        smart_answers = []
        url = 'https://www.gov.uk/api/search.json'
        url += '?filter_format=smart-answer'
        url += '&filter_format=simple_smart_answer'
        url += '&start=0&count=1000&fields=link'
        try:
            r = requests.get(url)
            if r.status_code == 200:
                results = r.json()['results']
                return [SmartAnswer(result['link'].encode('utf-8')) for result in results]
        except requests.exceptions.ConnectionError, requests.exceptions.HTTPError:
            print('ERROR ' + url, file=sys.stderr)


