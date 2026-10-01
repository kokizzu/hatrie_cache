#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatSql/procedure_registry.go hat/hatSql/t_u03_procedure_registry_test.go
