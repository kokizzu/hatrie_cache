#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatProcedure/registry.go hat/hatProcedure/registry_test.go
