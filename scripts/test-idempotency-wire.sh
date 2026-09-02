#!/usr/bin/env sh
set -eu

go test ./hat/hatCommand -run '^TestRequestIdempotencyKey'
