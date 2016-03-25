#!/bin/bash
set -e

# Starting Hive Metastore
echo "Starting Hive metastore."
hive --service metastore 1>> /mnt/logs/metastore.log 2>> /mnt/logs/metastore.log &
