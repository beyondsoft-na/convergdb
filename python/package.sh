#!/bin/bash

# this script runs the tests, then sanitizes the
# repo and creating the zip file.

# run integration tests
./this_pytest tests/*.py -vv

# sanitize
rm -f  convergdb/*.pyc
rm -rf convergdb/__pycache__
rm -f  convergdb/.DS_Store
rm -rf spark-warehouse
rm -rf metastore_db
rm -f  convergdb.zip
rm -f  derby.log

# create the zip
zip -vr convergdb.zip convergdb/
mv convergdb.zip ../lib/generators/
