Script to load aggregate statistics about rates of problem reports and searches
into the Performance Platform's `govuk-info/info-statistics` dataset. The data
which is sent to that dataset is also written to a local CSV file named for the
date range it covers.

The data could be used to power a dashboard of the pages with the most searches,
most problem reports etc, but for now the CSV report can be used to compare the
performance of pages on GOV.UK with widely differing pageview rates. There is no
frontend yet to display the dataset from the Performance Platform.

How it works
------------

The script fetches its source data from GOV.UK's search API and from the
Performance Platform (PP):

- the URLs for all pages of all smart answers and simple smart answers
(from the search API)
- the numbers of anonymous feedback (problem) reports per page on GOV.UK in the
last 6 weeks
(from PP's `govuk-info/page-contacts` dataset, fetched by starting letter `/a`-`/z`)
- the numbers of searches from pages on GOV.UK in the last 6 weeks
(from PP's `govuk-info/search-terms` dataset, fetched by starting letter `/a`-`/z`)
- the numbers of pageviews in the last 6 weeks for each page which appears in
the `page-contacts` and `search-terms` data
(from PP's `govuk-info/page-statistics` dataset, fetched individually per URL;
this makes around 7000 GET requests)

It then combines the datapoints for all pages of each smart answer and simple
smart answer so that the whole smart answer is represented by a single datapoint.

Each datapoint contains the problem report and search rates per 100,000 pageviews.

The aggregated datapoints are then written to a CSV file and `POST`ed to the
`govuk-info/info-statistics` PP dataset.

Datapoints for URLs which have previously appeared but which don't have any
associated data from the last 6 weeks are not deleted from the `info-statistics`
dataset, but the dates in the datapoints will indicate that they are no longer
current.

Instructions
------------

Environment variables for configuration:

- `PP_DATASET_TOKEN`: (required) a token for the `govuk-info/info-statistics` dataset
- `DATA_DOMAIN`: the base URL for the Performance Platform; defaults to
`https://www.performance.service.gov.uk/data`
- `LOG_LEVEL`: valid values: `DEBUG`, `INFO` (default), `WARNING`, `ERROR`, `CRITICAL`

To update data in the Performance Platform, use `./run.sh` (this script will
create its own virtualenv).

Development
-----------

Install the dependencies (you may want to do this inside a virtualenv):

    pip install -r requirements.txt

Set `PP_DATASET_TOKEN` as above (perhaps to something invalid) and call the
Python script directly (you may want to comment-out the POST to the Performance
Platform first):

    python -m stats.main

Testing
-------

Install the test dependencies (you may want to do this inside a virtualenv):

    pip install -r requirements_for_tests.txt

Run tests:

    nosetests

Troubleshooting
---------------

The script sometimes fails because requests to the Performance Platform time out
or otherwise fail (even though the
[PP client](https://github.com/alphagov/performanceplatform-client.py/blob/076848aa0a5a6ca4337d78c4647144843b9851d0/performanceplatform/client/base.py#L170-L173)
retries up to 5 times for 500, 502 and 503 reponses). It's safe to run the
script again if this happens, because it only makes a single POST request at the
end so the dataset cannot have been partially updated by the failure.
