#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatSchema/space_migration.go hat/hatSchema/space_migration_test.go
