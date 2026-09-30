#!/bin/sh
set -eu

cd /tmp/hatrie-cache-m234
go test -race -run '^TestM234' -count=1 ./hat/hatPipeline
