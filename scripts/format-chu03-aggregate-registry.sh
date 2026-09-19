#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatDataStructure/aggregate_state_registry.go \
  hat/hatDataStructure/aggregate_state_registry_test.go
