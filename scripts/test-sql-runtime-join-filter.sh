#!/bin/sh
set -eu

go test ./hat/hatSql -run 'TestRuntimeJoinBloomFilter' -count=1
