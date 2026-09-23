#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatSql/m226_consensus_metadata.go hat/hatSql/m226_consensus_metadata_test.go hat/hatSql/m226_consensus_metadata_benchmark_test.go
perl -0pi -e 's/\n+\z/\n/' M226_DURABLE_CONSENSUS_METADATA.md
