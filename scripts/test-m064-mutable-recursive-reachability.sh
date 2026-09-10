#!/usr/bin/env bash
set -eu

go test ./hat/hatSql -run 'Test(MutableRecursiveReachability|IncrementalRecursiveReachability)' -count=1
