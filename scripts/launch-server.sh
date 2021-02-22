#!/bin/bash
APP=/home/ubuntu/code/cowtracker-backend
cd ${APP}
pipenv run python -m cowtracker.server $@
exit 0

