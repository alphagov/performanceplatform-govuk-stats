# coding=utf-8

from datetime import date
import json
import logging
import re
import unittest
import urllib

from mock import patch, mock_open
import responses

from stats.info_statistics import InfoStatistics


# Prevent info/debug logging cluttering up test output
logging.disable(logging.INFO)


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
