from contextlib import contextmanager
import shutil
from tempfile import mkdtemp

from stats.info_statistics import Datapoint


def build_datapoint_with_counts(path):
    datapoint = Datapoint(path)
    datapoint.set_problem_reports_count(2)
    datapoint.set_search_count(5)
    datapoint.set_pageview_count(10)
    return datapoint


@contextmanager
def TemporaryDirectory():
    name = mkdtemp()
    try:
        yield name
    finally:
        shutil.rmtree(name)
