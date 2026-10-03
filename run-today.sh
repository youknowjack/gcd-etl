#!/bin/bash

# Change to the directory containing this script
cd "$(dirname "$0")"

DAY_INC=$1

# Get relative date in YYYY-MM-DD format
TODAY=$(/opt/homebrew/bin/gdate -d "+$DAY_INC day" +%Y-%m-%d)
echo $TODAY

# Read MySQL password from .mypw file and export it
if [ ! -f .mypw ]; then
    echo "Error: .mypw file not found"
    exit 1
fi
export mysqlpassword=$(cat .mypw)

# Call run-all.sh with today's date
./run-all.sh "$TODAY" >> run-all.log 2>&1
