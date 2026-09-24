#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatSchema/source_schema_registry.go hat/hatSchema/source_schema_registry_test.go
