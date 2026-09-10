#!/usr/bin/env bash
set -eu

go test -race ./hat/hatSql -run 'Test(MutableRecursiveReachability|IncrementalRecursiveReachability)' -count=10
