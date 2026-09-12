#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatSql/contracts.go \
  hat/hatSql/c216_columnar_dictionary_shape_test.go
