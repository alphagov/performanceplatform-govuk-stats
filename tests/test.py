from datetime import date, datetime
import json
import os
import re
import unittest
import urllib

import responses

from stats.info_statistics import (
    AggregatedDataset,
    Datapoint,
    GOVUK,
    InfoStatistics,
    PerformancePlatform,
    SmartAnswer
)


class TestGOVUK(unittest.TestCase):

    @responses.activate
    def testSmartanswerPathFetch(self):
        smart_answers = """
        {
          "results": [
            {
              "link": "/am-i-getting-minimum-wage"
            }
          ]
        }
        """

        url_re = re.compile(r'https://www.gov.uk/api/search.json\?filter_format=smart-answer.*?')
        responses.add(responses.GET, url_re,
                      body=smart_answers, status=200,
                      content_type='application/json')

        self.assertEqual(GOVUK().get_smart_answers(), [SmartAnswer("/am-i-getting-minimum-wage")])


class TestPerformancePlatform(unittest.TestCase):

    def testDatesAreFormattedAsMidnightWithNaiveDatetimes(self):
        pp = PerformancePlatform('foo',
                                 start_date=datetime(2014, 12, 16, 5, 45, 0),
                                 end_date=datetime(2015, 01, 27, 3, 27, 0))
        expected_start_date = "2014-12-16T00:00:00Z"
        expected_end_date = "2015-01-27T00:00:00Z"
        self.assertEqual(pp.start_date, expected_start_date)
        self.assertEqual(pp.end_date, expected_end_date)

    def testDatesAreFormattedAsMidnightWithNaiveDates(self):
        pp = PerformancePlatform('foo',
                                 start_date=date(2014, 12, 16),
                                 end_date=date(2015, 01, 27))
        expected_start_date = "2014-12-16T00:00:00Z"
        expected_end_date = "2015-01-27T00:00:00Z"
        self.assertEqual(pp.start_date, expected_start_date)
        self.assertEqual(pp.end_date, expected_end_date)

    @responses.activate
    def testProblemReportCountFetching(self):
        page_contacts = """
        {
          "data": [
            {
              "pagePath": "/academies-financial-returns",
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

        url_re = re.compile(r'https://www.performance.service.gov.uk/data/govuk-info/page-contacts.*?filter_by_prefix=pagePath%3A%2Fa') # pagePath:/a
        responses.add(responses.GET, url_re,
                      body=page_contacts, status=200,
                      content_type='application/json')

        url_re = re.compile(r'https://www.performance.service.gov.uk/data/govuk-info/page-contacts.*?')
        responses.add(responses.GET, url_re,
                      body='[]', status=200,
                      content_type='application/json')

        pp = PerformancePlatform('foo',
                                 start_date=date(2014, 12, 16),
                                 end_date=date(2015, 01, 27))

        expected_problem_report_counts = {
            "/academies-financial-returns": 5,
            "/am-i-getting-minimum-wage": 3,
            "/am-i-getting-minimum-wage/y": 1
        }
        self.assertEqual(pp.get_problem_report_counts(), expected_problem_report_counts)

    @responses.activate
    def testSearchCountFetching(self):
        searches = """
        {
          "data": [
            {
              "pagePath": "/academies-financial-returns",
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

        url_re = re.compile(r'https://www.performance.service.gov.uk/data/govuk-info/search-terms.*?filter_by_prefix=pagePath%3A%2Fa') # pagePath:/a
        responses.add(responses.GET, url_re,
                      body=searches, status=200,
                      content_type='application/json')

        url_re = re.compile(r'https://www.performance.service.gov.uk/data/govuk-info/search-terms.*?')
        responses.add(responses.GET, url_re,
                      body='[]', status=200,
                      content_type='application/json')

        pp = PerformancePlatform('foo',
                                 start_date=date(2014, 12, 16),
                                 end_date=date(2015, 01, 27))

        expected_search_counts = {
            "/academies-financial-returns": 10,
            "/am-i-getting-minimum-wage": 2,
            "/am-i-getting-minimum-wage/y": 1
        }
        self.assertEqual(pp.get_search_counts(), expected_search_counts)

    @responses.activate
    def testUniquePageviewFetching(self):
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
            "/am-i-getting-minimum-wage": 2000,
            "/am-i-getting-minimum-wage/y": 500
        }

        for path, pageview in expected_pageviews_calls.iteritems():
            url_re = re.compile(r'https://www.performance.service.gov.uk/data/govuk-info/page-statistics.*?'+ urllib.quote("pagePath:"+path, "") +".*?")
            responses.add(responses.GET, url_re,
                          body=page_statistics % (path, pageview), status=200,
                          content_type='application/json')
        pp = PerformancePlatform('foo',
                                 start_date=date(2014, 12, 16),
                                 end_date=date(2015, 01, 27))

        expected_pageview_counts = {
            "/academies-financial-returns": 1000,
            "/am-i-getting-minimum-wage": 2000,
            "/am-i-getting-minimum-wage/y": 500
        }
        self.assertEqual(pp.get_unique_pageviews(expected_pageview_counts.keys()), expected_pageview_counts)


class TestAggregatedDataset(unittest.TestCase):
    def testAggregatedDataset(self):
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
    def testDataProcessing(self):
        searches = """
        {
          "data": [
            {
              "pagePath": "/academies-financial-returns",
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
        page_contacts = """
        {
          "data": [
            {
              "pagePath": "/academies-financial-returns",
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
            "/am-i-getting-minimum-wage": 2000,
            "/am-i-getting-minimum-wage/y": 500
        }

        smart_answers = """
        {
          "results": [
            {
              "link": "/am-i-getting-minimum-wage"
            }
          ]
        }
        """

        url_re = re.compile(r'https://www.performance.service.gov.uk/data/govuk-info/search-terms.*?filter_by_prefix=pagePath%3A%2Fa') # pagePath:/a
        responses.add(responses.GET, url_re,
                      body=searches, status=200,
                      content_type='application/json')

        url_re = re.compile(r'https://www.performance.service.gov.uk/data/govuk-info/search-terms.*?')
        responses.add(responses.GET, url_re,
                      body='[]', status=200,
                      content_type='application/json')

        url_re = re.compile(r'https://www.performance.service.gov.uk/data/govuk-info/page-contacts.*?filter_by_prefix=pagePath%3A%2Fa') # pagePath:/a
        responses.add(responses.GET, url_re,
                      body=page_contacts, status=200,
                      content_type='application/json')

        url_re = re.compile(r'https://www.performance.service.gov.uk/data/govuk-info/page-contacts.*?')
        responses.add(responses.GET, url_re,
                      body='[]', status=200,
                      content_type='application/json')

        for path, pageview in expected_pageviews_calls.iteritems():
            url_re = re.compile(r'https://www.performance.service.gov.uk/data/govuk-info/page-statistics.*?'+ urllib.quote("pagePath:"+path, "") +".*?")
            responses.add(responses.GET, url_re,
                          body=page_statistics % (path, pageview), status=200,
                          content_type='application/json')

        url_re = re.compile(r'https://www.gov.uk/api/search.json\?filter_format=smart-answer.*?')
        responses.add(responses.GET, url_re,
                      body=smart_answers, status=200,
                      content_type='application/json')

        responses.add(responses.POST, 'https://www.performance.service.gov.uk/data/govuk-info/info-statistics',
                      body='{}',
                      content_type='application/json')

        self.info.process_data(logger=open(os.devnull, 'w'))

        # we're expecting:
        # - 26 GETs to PP: search terms (one for each letter of the alphabet)
        # - 26 GETs to PP: page contacts (one for each letter of the alphabet)
        # - 1 GET to PP: page statistics
        # - 1 GET to the GOV.UK content API
        # - 1 POST to PP: info-statistics
        self.assertEqual(len(responses.calls), 57)

        expectedAggregateReport = [
          {
            "_id": "_academies-financial-returns",
            "_timestamp": "2015-01-27T00:00:00Z",
            "_start_at": "2014-12-16T00:00:00Z",
            "_end_at": "2015-01-27T00:00:00Z",
            "searchesPer100kViews": 1000.0,
            "problemsPer100kViews": 500.0,
            "pagePath": "/academies-financial-returns",
            "searchUniques": 10.0,
            "problemReports": 5.0,
            "uniquePageviews": 1000
          },
          {
            "_id": "_am-i-getting-minimum-wage",
            "_timestamp": "2015-01-27T00:00:00Z",
            "_start_at": "2014-12-16T00:00:00Z",
            "_end_at": "2015-01-27T00:00:00Z",
            "searchesPer100kViews": 150.0,
            "problemsPer100kViews": 200.0,
            "pagePath": "/am-i-getting-minimum-wage",
            "searchUniques": 3.0,
            "problemReports": 4.0,
            "uniquePageviews": 2000
          }
        ]

        self.assertEqual(json.loads(responses.calls[-1].request.body), expectedAggregateReport)
