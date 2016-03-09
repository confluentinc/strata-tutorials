#!/usr/bin/env bash

awk /^[[:digit:]]/ yellow_tripdata_2015-12.csv > yellow_tripdata_2015-12.nohdr.csv