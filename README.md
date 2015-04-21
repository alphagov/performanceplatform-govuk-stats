Script to load aggregate statistics about rates of problem reports and searches
into the Performance Platform's /info-statistics dataset.

The data could be used to power a dashboard of the pages with the most searches,
most problem reports etc.

Instructions
------------

To update data in the Performance Platform:

- Set the `PP_DATASET_TOKEN` environment variable (and optionally `DATA_DOMAIN`)
- Run `run.sh` (this script will create its own virtualenv)

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
