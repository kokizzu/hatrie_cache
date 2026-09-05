#!/bin/sh
set -eu

gofmt -w hat/hatSql/row_binary_delta_codec.go hat/hatSql/row_binary_delta_codec_test.go
