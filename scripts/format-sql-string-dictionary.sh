#!/bin/sh
set -eu

gofmt -w hat/hatSql/string_dictionary.go hat/hatSql/string_dictionary_test.go
