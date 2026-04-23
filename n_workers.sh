#!/bin/bash

trap "kill 0" EXIT

for i in $(seq 1 $1)
do
    python -u  Worker.py --name "worker$i" > "worker$i.log" &
done

wait
