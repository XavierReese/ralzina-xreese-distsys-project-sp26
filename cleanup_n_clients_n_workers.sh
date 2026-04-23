#!/bin/bash

# Usage: bash cleanup <n_clients_to_cleanup> <n_workers_to_cleanup>

for i in $(seq 1 $1)
do
	rm -r "client$i"
done

for i in $(seq 1 $2)
do
	rm -r "worker${i}_dir"
done

rm *.log

rm -r coordinator_jobs
