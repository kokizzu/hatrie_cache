#!/usr/bin/env bash
set -eu

rg -n "CH-46|Kafka.*offset|ch-046|CH046" INSPIRATION_BACKLOG.md README.md BENCHMARK.md
sed -n '1,28p' BENCHMARK.md
