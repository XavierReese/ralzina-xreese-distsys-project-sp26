#!/bin/bash

trap "kill 0" EXIT

for i in $(seq 1 $1)
do
	python -u Client.py --name "client$i" < <(sleep 5; tail -f client_input.txt) > "client$i.log" &
done

wait
