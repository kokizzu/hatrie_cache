#!/usr/bin/env bash
set -euo pipefail

nl -ba hat/hatTopology/tu14_bucket_migration.go
printf '\n--- ownership helpers ---\n'
sed -n '1,85p' hat/hatTopology/ownership.go
printf '\n--- topology fingerprint ---\n'
sed -n '261,315p' hat/hatTopology/topology.go
printf '\n--- benchmark document header ---\n'
sed -n '1,30p' BENCHMARK.md
