#!/usr/bin/env bash
set -eu

rg -n -C 4 -i "MZ-14|MZ-15|schema.?registry|confluent|avro" \
  INSPIRATION_BACKLOG.md CLICKHOUSE_MATERIALIZE_TARANTOOL_AUDIT.md
