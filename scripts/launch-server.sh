#!/bin/bash
APP=/home/ubuntu/code/cowtracker_app
cd ${APP}
pipenv run python -m cowtracker.server $@
exit 0

