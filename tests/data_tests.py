import logging
import unittest

from .helpers import build_datapoint_with_counts
from stats.data import AggregatedDataset


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
