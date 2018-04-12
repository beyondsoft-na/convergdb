#!/usr/bin/env bash

./csv_to_json.py -i s3://irs-form-990/index_2011.csv -o s3://irs990-source.beyondsoft.us/index_2011.json --region us-west-2 --profile beyondsoft --verbose
./csv_to_json.py -i s3://irs-form-990/index_2012.csv -o s3://irs990-source.beyondsoft.us/index_2012.json --region us-west-2 --profile beyondsoft --verbose
./csv_to_json.py -i s3://irs-form-990/index_2013.csv -o s3://irs990-source.beyondsoft.us/index_2013.json --region us-west-2 --profile beyondsoft --verbose
./csv_to_json.py -i s3://irs-form-990/index_2014.csv -o s3://irs990-source.beyondsoft.us/index_2014.json --region us-west-2 --profile beyondsoft --verbose
./csv_to_json.py -i s3://irs-form-990/index_2015.csv -o s3://irs990-source.beyondsoft.us/index_2015.json --region us-west-2 --profile beyondsoft --verbose
./csv_to_json.py -i s3://irs-form-990/index_2016.csv -o s3://irs990-source.beyondsoft.us/index_2016.json --region us-west-2 --profile beyondsoft --verbose
#./csv_to_json.py -i s3://irs-form-990/index_2017.csv -o s3://irs990-source.beyondsoft.us/index_2017.json --region us-west-2 --profile beyondsoft --verbose
