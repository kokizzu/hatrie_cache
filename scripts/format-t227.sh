#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatSchema/field_validation.go \
  hat/hatSchema/t227_field_validation_test.go
