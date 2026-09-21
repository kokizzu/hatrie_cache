#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatReplication -run '^$' -bench 'BenchmarkTU047(ClusterWriteCommit|ExistingWriteQuorum)' -benchmem -count=3
