#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatSql/m225_shard_leases.go hat/hatSql/m225_shard_leases_test.go
