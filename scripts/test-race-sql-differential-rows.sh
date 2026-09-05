#!/bin/sh
set -eu

go test -race ./hat/hatSql -run 'TestConsolidateDifferentialRows' -count=1
