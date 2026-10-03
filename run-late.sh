#!/bin/bash

# Change to the directory containing this script
cd "$(dirname "$0")"

# Get tomorrow's date in YYYY-MM-DD format
TODAY=$1

# Read MySQL password from .mypw file and export it
if [ ! -f .mypw ]; then
    echo "Error: .mypw file not found"
    exit 1
fi
export mysqlpassword=$(cat .mypw)

# Call run-all.sh with today's date
./run-all.sh "$TODAY" >> run-all.log 2>&1
