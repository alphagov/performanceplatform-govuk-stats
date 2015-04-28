from datetime import datetime
import logging
import os
import unittest

from .helpers import build_datapoint_with_counts, TemporaryDirectory
from stats.csv_writer import CSVWriter


# Prevent info/debug logging cluttering up test output
logging.disable(logging.INFO)


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
