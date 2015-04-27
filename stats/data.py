import logging


logger = logging.getLogger(__name__)


class Datapoint(object):
    data_fields = ['uniquePageviews', 'problemReports', 'searchUniques', 'pagePath']
    calculated_fields = ['_id', 'problemsPer100kViews', 'searchesPer100kViews']
    all_fields = data_fields + calculated_fields

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
        return {key: self[key] for key in self.all_fields}

    def __getitem__(self, item):
        if item == 'problemsPer100kViews':
            return self._contact_rate()
        elif item == 'searchesPer100kViews':
            return self._search_rate()
        elif item == '_id':
            return self.get_path().replace('/', '_').replace(' ', '%20')
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
        logger.info('Aggregating datapoints')
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
