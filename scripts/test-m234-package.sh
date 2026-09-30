#!/bin/sh
set -eu

cd /tmp/hatrie-cache-m234
go test ./hat/hatPipeline
