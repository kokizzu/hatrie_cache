#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatDataStructure/sealed_upsert_run.go hat/hatDataStructure/sealed_upsert_run_test.go
