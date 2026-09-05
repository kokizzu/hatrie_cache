#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatSql/row_binary_dictionary.go hat/hatSql/row_binary_dictionary_test.go
