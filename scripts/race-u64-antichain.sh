#!/usr/bin/env bash
set -eu

go test -race -count=1 ./hat/hatDataStructure -run 'TestU64Antichain'
