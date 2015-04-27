# coding=utf-8

from datetime import date, datetime
import json
import logging
import os
import re
import unittest
import urllib

from mock import patch, mock_open
import responses

from .helpers import build_datapoint_with_counts, TemporaryDirectory
from stats.data import AggregatedDataset
from stats.info_statistics import CSVWriter, InfoStatistics


# Prevent info/debug logging cluttering up test output
logging.disable(logging.INFO)


class TestDatapoint(unittest.TestCase):
    def setUp(self):
        self.datapoint = build_datapoint_with_counts('/i/am/a path')

    def test_getters_and_setters(self):
        self.assertEqual(2, self.datapoint.get_problem_reports_count())
        self.assertEqual(5, self.datapoint.get_search_count())
        self.assertEqual(10, self.datapoint.get_pageview_count())
        self.assertEqual('/i/am/a path', self.datapoint.get_path())

    def test_id_replaces_slashes_and_spaces(self):
        self.assertEqual('_i_am_a%20path', self.datapoint['_id'])

    def test_as_dict(self):
        expected_dict = {
            '_id': '_i_am_a%20path',
            'pagePath': '/i/am/a path',
            'problemReports': 2,
            'problemsPer100kViews': 20000.0,
            'searchUniques': 5,
            'searchesPer100kViews': 50000.0,
            'uniquePageviews': 10,
        }
        self.assertEqual(expected_dict, self.datapoint.as_dict())


class TestCSVWriter(unittest.TestCase):
    def test_default_filename(self):
        start_date = datetime(2015, 2, 3, 9, 15, 30)
        end_date = datetime(2015, 3, 6, 14, 27, 17)
        writer = CSVWriter(start_date=start_date, end_date=end_date)

        expected_filename = 'report_2015-02-03_2015-03-06.csv'
        self.assertEqual(writer.output_filename, expected_filename)

    def test_writing_csv(self):
        datapoints = [
            build_datapoint_with_counts('/path1'),
            build_datapoint_with_counts('/path2'),
        ]

        expected_csv_lines = [
            'uniquePageviews,problemReports,searchUniques,pagePath,_id,problemsPer100kViews,searchesPer100kViews',
            '10,2,5,/path1,_path1,20000.0,50000.0',
            '10,2,5,/path2,_path2,20000.0,50000.0',
        ]

        with TemporaryDirectory() as tempdir:
            csv_filename = os.path.join(tempdir, 'test_report.csv')
            writer = CSVWriter(output_filename=csv_filename)
            writer.write_datapoints(datapoints)

            with open(csv_filename, 'r') as open_file:
                file_lines = open_file.read().splitlines()
                self.assertEqual(file_lines, expected_csv_lines)


class TestAggregatedDataset(unittest.TestCase):
    def test_aggregated_dataset(self):
        aggregate = AggregatedDataset()
        aggregate.add_problem_report_counts({'/abc':2, '/def':3})
        aggregated_points = aggregate.get_aggregated_datapoints()

        self.assertEqual(aggregated_points["/abc"]["pagePath"], "/abc")
        self.assertEqual(aggregated_points["/abc"]["problemReports"], 2)
        self.assertEqual(aggregated_points["/abc"]["searchUniques"], 0)
        self.assertEqual(aggregated_points["/abc"]["uniquePageviews"], 0)
        self.assertEqual(aggregated_points["/abc"]["problemsPer100kViews"], None)
        self.assertEqual(aggregated_points["/abc"]["searchesPer100kViews"], None)

        self.assertEqual(aggregated_points["/def"]["pagePath"], "/def")
        self.assertEqual(aggregated_points["/def"]["problemReports"], 3)
        self.assertEqual(aggregated_points["/def"]["searchUniques"], 0)
        self.assertEqual(aggregated_points["/def"]["uniquePageviews"], 0)
        self.assertEqual(aggregated_points["/def"]["problemsPer100kViews"], None)
        self.assertEqual(aggregated_points["/def"]["searchesPer100kViews"], None)

        aggregate.add_search_counts({'/def':5, '/xyz':10})
        aggregated_points = aggregate.get_aggregated_datapoints()

        self.assertEqual(aggregated_points["/def"]["problemReports"], 3)
        self.assertEqual(aggregated_points["/def"]["searchUniques"], 5)
        self.assertEqual(aggregated_points["/def"]["problemsPer100kViews"], None)
        self.assertEqual(aggregated_points["/def"]["searchesPer100kViews"], None)

        self.assertEqual(aggregated_points["/xyz"]["problemReports"], 0)
        self.assertEqual(aggregated_points["/xyz"]["searchUniques"], 10)
        self.assertEqual(aggregated_points["/xyz"]["searchesPer100kViews"], None)

        aggregate.add_unique_pageviews({'/abc':2000, '/def':4000, '/xyz':8000})
        aggregated_points = aggregate.get_aggregated_datapoints()

        self.assertEqual(aggregated_points["/abc"]["uniquePageviews"], 2000)
        self.assertEqual(aggregated_points["/def"]["uniquePageviews"], 4000)
        self.assertEqual(aggregated_points["/xyz"]["uniquePageviews"], 8000)

        self.assertEqual(aggregated_points["/abc"]["problemsPer100kViews"], 100.0)
        self.assertEqual(aggregated_points["/xyz"]["problemsPer100kViews"], None)

        self.assertEqual(aggregated_points["/def"]["searchesPer100kViews"], 125.0)
        self.assertEqual(aggregated_points["/xyz"]["searchesPer100kViews"], 125.0)


class TestInfoStatistics(unittest.TestCase):

    def setUp(self):
        self.info = InfoStatistics('foo',
                                   start_date=date(2014, 12, 16),
                                   end_date=date(2015, 01, 27))

    @responses.activate
    @patch('__builtin__.open', new=mock_open())
    def test_data_processing(self):
        searches = """
        {
          "data": [
            {
              "pagePath": "/academies-financial-returns",
              "searchUniques:sum": 10.0
            },
            {
              "pagePath": "/am-i-getting-minimum-wag€",
              "searchUniques:sum": 2.0
            },
            {
              "pagePath": "/am-i-getting-minimum-wag€/y",
              "searchUniques:sum": 1.0
            }
          ]
        }
        """
        page_contacts = """
        {
          "data": [
            {
              "pagePath": "/academies-financial-returns",
              "total:sum": 5.0
            },
            {
              "pagePath": "/am-i-getting-minimum-wag€",
              "total:sum": 3.0
            },
            {
              "pagePath": "/am-i-getting-minimum-wag€/y",
              "total:sum": 1.0
            }
          ]
        }
        """

        page_statistics = """
        {
          "data": [
            {
              "pagePath": "%s",
              "uniquePageviews:sum": %.01f
            }
          ]
        }
        """

        expected_pageviews_calls = {
            "/academies-financial-returns": 1000,
            "/am-i-getting-minimum-wag€": 2000,
            "/am-i-getting-minimum-wag€/y": 500
        }

        smart_answers = """
        {
          "results": [
            {
              "link": "/am-i-getting-minimum-wag€"
            }
          ]
        }
        """

        url_re = re.compile(
            r'https://www.performance.service.gov.uk/data/govuk-info/search-terms.*?filter_by_prefix=pagePath%3A%2Fa'
        ) # pagePath:/a
        responses.add(responses.GET, url_re,
                      body=searches, status=200,
                      content_type='application/json')

        url_re = re.compile(
            r'https://www.performance.service.gov.uk/data/govuk-info/search-terms.*?'
        )
        responses.add(responses.GET, url_re,
                      body='[]', status=200,
                      content_type='application/json')

        url_re = re.compile(
            r'https://www.performance.service.gov.uk/data/govuk-info/page-contacts.*?filter_by_prefix=pagePath%3A%2Fa'
        ) # pagePath:/a
        responses.add(responses.GET, url_re,
                      body=page_contacts, status=200,
                      content_type='application/json')

        url_re = re.compile(
            r'https://www.performance.service.gov.uk/data/govuk-info/page-contacts.*?'
        )
        responses.add(responses.GET, url_re,
                      body='[]', status=200,
                      content_type='application/json')

        for path, pageview in expected_pageviews_calls.iteritems():
            url_re = re.compile(
                r'https://www.performance.service.gov.uk/data/govuk-info/page-statistics.*?' + urllib.quote("pagePath:" + path, "") + ".*?"
            )
            responses.add(responses.GET, url_re,
                          body=page_statistics % (path, pageview), status=200,
                          content_type='application/json')

        url_re = re.compile(
            r'https://www.gov.uk/api/search.json\?filter_format=smart-answer&filter_format=simple_smart_answer.*?'
        )
        responses.add(responses.GET, url_re,
                      body=smart_answers, status=200,
                      content_type='application/json')

        responses.add(responses.POST,
                      'https://www.performance.service.gov.uk/data/govuk-info/info-statistics',
                      body='{}',
                      content_type='application/json')

        self.info.process_data()

        # we're expecting:
        # - 26 GETs to PP: search terms (one for each letter of the alphabet)
        # - 26 GETs to PP: page contacts (one for each letter of the alphabet)
        # - 1 GET to PP: page statistics
        # - 1 GET to the GOV.UK content API
        # - 1 POST to PP: info-statistics
        self.assertEqual(len(responses.calls), 57)

        expectedAggregateReport = [
          {
            u"_id": u"_am-i-getting-minimum-wag€",
            u"_timestamp": u"2015-01-27T00:00:00Z",
            u"_start_at": u"2014-12-16T00:00:00Z",
            u"_end_at": u"2015-01-27T00:00:00Z",
            u"searchesPer100kViews": 150.0,
            u"problemsPer100kViews": 200.0,
            u"pagePath": u"/am-i-getting-minimum-wag€",
            u"searchUniques": 3.0,
            u"problemReports": 4.0,
            u"uniquePageviews": 2000
          },
          {
            u"_id": u"_academies-financial-returns",
            u"_timestamp": u"2015-01-27T00:00:00Z",
            u"_start_at": u"2014-12-16T00:00:00Z",
            u"_end_at": u"2015-01-27T00:00:00Z",
            u"searchesPer100kViews": 1000.0,
            u"problemsPer100kViews": 500.0,
            u"pagePath": u"/academies-financial-returns",
            u"searchUniques": 10.0,
            u"problemReports": 5.0,
            u"uniquePageviews": 1000
          }
        ]

        posted_body = json.loads(responses.calls[-1].request.body)
        self.assertEqual(posted_body, expectedAggregateReport)
