#!/bin/sh

CMD='python3 scripts/insert_random_meas.py --batt-cap 100'

for i in {48,49,50,51,52,53,54,55,56,57,304,305}
do
    $CMD --id $i $@
done