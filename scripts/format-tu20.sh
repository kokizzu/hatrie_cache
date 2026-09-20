#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
	hat/hatDataStructure/online_space_upgrade.go \
	hat/hatDataStructure/tu20_online_space_upgrade_baseline_benchmark_test.go \
	hat/hatDataStructure/tu20_online_space_upgrade_test.go \
	hat/hatDataStructure/tu20_online_space_upgrade_benchmark_test.go
