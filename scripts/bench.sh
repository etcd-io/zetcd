#!/bin/bash

set -eo pipefail

function tidy {
	kill -9 `pgrep etcd` || true
	kill -9 `pgrep zetcd` || true
	docker kill `docker ps | egrep "(zookeeper|kafka)" | awk ' { print $1 } '` || true
}

function run_config {
	#config="xchk.kafka"
	config="$1"
	workload="$2"
	tidy
	sleep 1s
	goreman -f scripts/Procfile."$config".$workload start &
	pid=$!
	sleep 10s
	./scripts/bench-$workload.sh >bench.$config.$workload
	kill $pid
}

for a in zetcd xchk zk; do run_config $a kafka; done
tidy