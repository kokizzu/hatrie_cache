#!/usr/bin/env bash
set -eu

rg -n "MZ-15|Protobuf schema|Confluent|mz-015|MZ015" \
  MZ015_PROTOBUF_SCHEMA_REGISTRY.md README.md INSPIRATION_BACKLOG.md BENCHMARK.md
