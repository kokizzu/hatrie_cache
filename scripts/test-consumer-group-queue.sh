#!/usr/bin/env bash
set -euo pipefail

mode=${1:-test}
case "$mode" in
test)
	go test ./hat/hatDataStructure -run 'TestConsumerGroupQueue' -count=1
	;;
test-all)
	go test ./hat/hatDataStructure
	;;
race)
	go test -race ./hat/hatDataStructure -run 'TestConsumerGroupQueue' -count=1
	;;
vet)
	go vet ./hat/hatDataStructure
	;;
benchmark)
	go test ./hat/hatDataStructure -run '^$' -bench 'BenchmarkConsumerGroupQueueT43' -benchmem -benchtime=200ms -count=5
	;;
format)
	gofmt -w hat/hatDataStructure/consumer_group_queue.go hat/hatDataStructure/consumer_group_queue_test.go hat/hatDataStructure/consumer_group_queue_benchmark_test.go hat/hatDataStructure/consumer_group_queue_setup_benchmark_test.go
	;;
*)
	echo "unknown mode: $mode" >&2
	exit 2
	;;
esac
