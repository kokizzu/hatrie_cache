#!/bin/sh
set -eu

gofmt -w hat/hatSql/row_binary_adaptive_codec.go hat/hatSql/row_binary_adaptive_codec_test.go
