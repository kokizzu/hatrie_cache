#!/usr/bin/env bash
set -eu

rg -n "MZ-14|Avro schema|Confluent|mz-014|MZ014" \
  MZ014_AVRO_SCHEMA_REGISTRY.md README.md INSPIRATION_BACKLOG.md BENCHMARK.md
