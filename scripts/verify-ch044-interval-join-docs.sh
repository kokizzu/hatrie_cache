#!/usr/bin/env bash
set -eu

test -s CH044_INTERVAL_JOIN_MAINTENANCE.md
grep -q 'CH-44' CH044_INTERVAL_JOIN_MAINTENANCE.md
grep -q 'ch-44-interval-join-maintenance' BENCHMARK.md
grep -q 'CH044_INTERVAL_JOIN_MAINTENANCE.md' README.md
grep -q '| CH-44 | Interval join maintenance' INSPIRATION_BACKLOG.md
