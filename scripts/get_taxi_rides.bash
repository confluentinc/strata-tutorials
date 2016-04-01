#!/usr/bin/env bash

pushd data
    if [ ! -e yellow_tripdata_2015-12.nohdr.csv ]; then
        curl https://storage.googleapis.com/tlc-trip-data/2015/yellow_tripdata_2015-12.csv \
            --output yellow_tripdata_2015-12.csv
        awk /^[[:digit:]]/ yellow_tripdata_2015-12.csv > yellow_tripdata_2015-12.nohdr.csv
        rm yellow_tripdata_2015-12.csv
    fi
    cat ../src/sql/taxis.sql | sqlite3 cabsDec2015.db
popd