#!/bin/sh

set -e

rm -rf ./venv

virtualenv --no-site-packages ./venv
echo 'Installing packages...'
./venv/bin/pip -q install --download-cache "${HOME}/bundles/${JOB_NAME}" -r requirements.txt

echo 'Updating data...'
./venv/bin/python -m stats.main

echo 'Done'
rm -rf ./venv
exit 0
