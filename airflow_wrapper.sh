#! /usr/bin/env bash

# run the script that calculates the appropriate time to fake
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
OFFSET_TIME="$(python $DIR/containertime.py $FIRST_FAKED_TIME)"

# run airflow, faked at that time
faketime "$OFFSET_TIME" actual_airflow $@