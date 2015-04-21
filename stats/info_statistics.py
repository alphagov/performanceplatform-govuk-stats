from __future__ import print_function

import copy
from datetime import datetime, timedelta
import io
import itertools
import json
import os
import string
import sys

from performanceplatform.client import DataSet
import requests

import settings


class Datapoint(object):
    data_fields = ['uniquePageviews', 'problemReports', 'searchUniques', 'pagePath']
    calculated_fields = ['_id', 'problemsPer100kViews', 'searchesPer100kViews']

    def __init__(self, path):
        self.data = {field: 0 for field in self.data_fields}
        self.data['pagePath'] = path

    def set_problem_reports_count(self, count):
        self.data['problemReports'] = count

    def get_problem_reports_count(self):
        return self.data['problemReports']

    def set_search_count(self, count):
        self.data['searchUniques'] = count

    def get_search_count(self):
        return self.data['searchUniques']

    def set_pageview_count(self, count):
        self.data['uniquePageviews'] = count

    def get_pageview_count(self):
        return self.data['uniquePageviews']

    def get_path(self):
        return self.data['pagePath']

    def as_dict(self):
        return {key: self[key] for key in (self.data_fields + self.calculated_fields)}

    def __getitem__(self, item):
        if item == 'problemsPer100kViews':
            return self._contact_rate()
        elif item == 'searchesPer100kViews':
            return self._search_rate()
        elif item == '_id':
            return self.get_path().replace('/', '_')
        else:
            return self.data[item]

    def _contact_rate(self):
        if (self.data["uniquePageviews"]
                and self.data["problemReports"]
                and self.data["uniquePageviews"] > 0):
            return float(self.data['problemReports'] * 100000) / self.data['uniquePageviews']

    def _search_rate(self):
        if (self.data["uniquePageviews"]
                and self.data["searchUniques"]
                and self.data["uniquePageviews"] > 0):
            return float(self.data['searchUniques'] * 100000) / self.data['uniquePageviews']


class AggregatedDataset(object):

    def __init__(self):
        self.entries = {}

    def add_problem_report_counts(self, problem_reports):
        for path, problem_report_count in problem_reports.iteritems():
            self[path].set_problem_reports_count(problem_report_count)

    def add_search_counts(self, search_counts):
        for path, search_count in search_counts.iteritems():
            self[path].set_search_count(search_count)

    def add_unique_pageviews(self, pageviews):
        for path, pageview_count in pageviews.iteritems():
            self[path].set_pageview_count(pageview_count)

    def get_aggregated_datapoints(self):
        return self.entries

    def __getitem__(self, path):
        if path not in self.entries:
            self.entries[path] = Datapoint(path)
        return self.entries[path]


class SmartAnswer(object):

    def __init__(self, path):
        self.path = path

    def includes(self, path):
        return path.startswith(self.path)

    def __cmp__(self, other):
        return self.path != other.path

    def combine_datapoints(self, datapoints):
        combined_datapoint = Datapoint(self.path)

        total_problem_reports_count = sum(datapoint.get_problem_reports_count()
                                          for datapoint in datapoints)
        total_search_count = sum(datapoint.get_search_count()
                                 for datapoint in datapoints)
        max_pageview_count = max(datapoint.get_pageview_count()
                                 for datapoint in datapoints)

        combined_datapoint.set_problem_reports_count(total_problem_reports_count)
        combined_datapoint.set_search_count(total_search_count)
        combined_datapoint.set_pageview_count(max_pageview_count)
        return combined_datapoint


class AggregatedDatasetCombiningSmartAnswers(object):

    def __init__(self, smartanswers):
        self.underlying_dataset = AggregatedDataset()
        self.smartanswers = smartanswers

    def add_problem_report_counts(self, problem_reports):
        self.underlying_dataset.add_problem_report_counts(problem_reports)

    def add_search_counts(self, search_counts):
        self.underlying_dataset.add_search_counts(search_counts)

    def add_unique_pageviews(self, pageviews):
        self.underlying_dataset.add_unique_pageviews(pageviews)

    def get_aggregated_datapoints(self):
        datapoints = self.underlying_dataset.get_aggregated_datapoints()

        for smartanswer in self.smartanswers:
            datapoints_for_smartanswer = [dp for path, dp in datapoints.items()
                                          if smartanswer.includes(path)]
            if datapoints_for_smartanswer:
                self._replace(datapoints, datapoints_for_smartanswer,
                              smartanswer.combine_datapoints(datapoints_for_smartanswer))

        return datapoints

    def _replace(self, all_datapoints, datapoints_to_remove, datapoint_to_add):
        for datapoint in datapoints_to_remove:
            all_datapoints.pop(datapoint.get_path(), None)

        all_datapoints[datapoint_to_add.get_path()] = datapoint_to_add


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
        results_by_letter = [self._get_problem_report_counts_for_paths_starting_with('/' + letter)
                             for letter in string.lowercase]
        all_results = list(itertools.chain(*results_by_letter))
        return {result["pagePath"].encode('utf-8'): result["total:sum"]
                for result in all_results}

    def get_search_counts(self):
        results_by_letter = [self._get_search_counts_for_paths_starting_with('/' + letter)
                             for letter in string.lowercase]
        all_results = list(itertools.chain(*results_by_letter))
        return {result["pagePath"].encode('utf-8'): result["searchUniques:sum"]
                for result in all_results}

    def get_unique_pageviews(self, paths):
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

        json_data = dataset.get(query_parameters)

        if 'data' in json_data:
            return json_data['data']
        else:
            return []


class GOVUK(object):

    def get_smart_answers(self):
        """Get all smart answers, from the Search API."""
        smart_answers = []
        url = 'https://www.gov.uk/api/search.json?filter_format=smart-answer'
        url += '&start=0&count=1000&fields=link'
        try:
            r = requests.get(url)
            if r.status_code == 200:
                results = r.json()['results']
                return [SmartAnswer(result['link'].encode('utf-8')) for result in results]
        except requests.exceptions.ConnectionError, requests.exceptions.HTTPError:
            print('ERROR ' + url, file=sys.stderr)


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
    - Write output to a local JSON file and to the PP
    """

    def __init__(self, pp_token, start_date=None, end_date=None):
        """
        Start and end dates are assumed to be UTC. They can be dates or datetimes.
        """
        self.end_date = end_date or datetime.utcnow()
        self.start_date = start_date or (self.end_date - timedelta(days=settings.DAYS))
        self.pp_adapter = PerformancePlatform(pp_token, self.start_date, self.end_date)

    def process_data(self, logger=sys.stdout):
        smart_answers = GOVUK().get_smart_answers()
        dataset = self._load_performance_data(smart_answers)

        aggregated_datapoints = dataset.get_aggregated_datapoints().values()

        print('Posting data to PP...', file=logger)
        self.pp_adapter.save_aggregated_results(aggregated_datapoints)

    def _load_performance_data(self, smart_answers):
        dataset = AggregatedDatasetCombiningSmartAnswers(smart_answers)
        problem_report_counts = self.pp_adapter.get_problem_report_counts()
        search_counts = self.pp_adapter.get_search_counts()
        involved_paths = list(set(problem_report_counts.keys() + search_counts.keys()))
        unique_pageviews = self.pp_adapter.get_unique_pageviews(involved_paths)

        dataset.add_unique_pageviews(unique_pageviews)
        dataset.add_problem_report_counts(problem_report_counts)
        dataset.add_search_counts(search_counts)

        return dataset
