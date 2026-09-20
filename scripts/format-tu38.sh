#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatReplication/tu38_conflict_introspection.go hat/hatReplication/tu38_conflict_introspection_test.go
