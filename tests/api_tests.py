# coding=utf-8

from datetime import date, datetime
import logging
import re
import unittest
import urllib

import responses

from stats.api import GOVUK, PerformancePlatform
from stats.data import SmartAnswer


# Prevent info/debug logging cluttering up test output
logging.disable(logging.INFO)


class TestGOVUK(unittest.TestCase):

    @responses.activate
    def test_smartanswer_path_fetch(self):
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
            r'https://www.gov.uk/api/search.json\?filter_format=smart-answer&filter_format=simple_smart_answer.*?'
        )
        responses.add(responses.GET, url_re,
                      body=smart_answers, status=200,
                      content_type='application/json')

        self.assertEqual(GOVUK().get_smart_answers(),
                         [SmartAnswer("/am-i-getting-minimum-wag€")])


class TestPerformancePlatform(unittest.TestCase):

    def test_dates_are_formatted_as_midnight_with_naive_datetimes(self):
        pp = PerformancePlatform('foo',
                                 start_date=datetime(2014, 12, 16, 5, 45, 0),
                                 end_date=datetime(2015, 01, 27, 3, 27, 0))
        expected_start_date = "2014-12-16T00:00:00Z"
        expected_end_date = "2015-01-27T00:00:00Z"
        self.assertEqual(pp.start_date, expected_start_date)
        self.assertEqual(pp.end_date, expected_end_date)

    def test_dates_are_formatted_as_midnight_with_naive_dates(self):
        pp = PerformancePlatform('foo',
                                 start_date=date(2014, 12, 16),
                                 end_date=date(2015, 01, 27))
        expected_start_date = "2014-12-16T00:00:00Z"
        expected_end_date = "2015-01-27T00:00:00Z"
        self.assertEqual(pp.start_date, expected_start_date)
        self.assertEqual(pp.end_date, expected_end_date)

    @responses.activate
    def test_problem_report_count_fetching(self):
        page_contacts = """
        {
          "data": [
            {
              "pagePath": "/academi€s-financial-returns",
              "total:sum": 5.0
            },
            {
              "pagePath": "/am-i-getting-minimum-wage",
              "total:sum": 3.0
            },
            {
              "pagePath": "/am-i-getting-minimum-wage/y",
              "total:sum": 1.0
            }
          ]
        }
        """

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

        pp = PerformancePlatform('foo',
                                 start_date=date(2014, 12, 16),
                                 end_date=date(2015, 01, 27))

        expected_problem_report_counts = {
            "/academi€s-financial-returns": 5,
            "/am-i-getting-minimum-wage": 3,
            "/am-i-getting-minimum-wage/y": 1
        }
        self.assertEqual(pp.get_problem_report_counts(),
                         expected_problem_report_counts)

    @responses.activate
    def test_search_count_fetching(self):
        searches = """
        {
          "data": [
            {
              "pagePath": "/academi€s-financial-returns",
              "searchUniques:sum": 10.0
            },
            {
              "pagePath": "/am-i-getting-minimum-wage",
              "searchUniques:sum": 2.0
            },
            {
              "pagePath": "/am-i-getting-minimum-wage/y",
              "searchUniques:sum": 1.0
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

        pp = PerformancePlatform('foo',
                                 start_date=date(2014, 12, 16),
                                 end_date=date(2015, 01, 27))

        expected_search_counts = {
            "/academi€s-financial-returns": 10,
            "/am-i-getting-minimum-wage": 2,
            "/am-i-getting-minimum-wage/y": 1
        }
        self.assertEqual(pp.get_search_counts(), expected_search_counts)

    @responses.activate
    def test_unique_pageview_fetching(self):
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
            "/academi€s-financial-returns": 1000,
            "/am-i-getting-minimum-wage": 2000,
            "/am-i-getting-minimum-wage/y": 500
        }

        for path, pageview in expected_pageviews_calls.iteritems():
            url_re = re.compile(
                r'https://www.performance.service.gov.uk/data/govuk-info/page-statistics.*?' + urllib.quote("pagePath:" + path, "") + ".*?"
            )
            responses.add(responses.GET, url_re,
                          body=page_statistics % (path, pageview), status=200,
                          content_type='application/json')
        pp = PerformancePlatform('foo',
                                 start_date=date(2014, 12, 16),
                                 end_date=date(2015, 01, 27))

        expected_pageview_counts = {
            "/academi€s-financial-returns": 1000,
            "/am-i-getting-minimum-wage": 2000,
            "/am-i-getting-minimum-wage/y": 500
        }
        self.assertEqual(pp.get_unique_pageviews(expected_pageview_counts.keys()),
                         expected_pageview_counts)
