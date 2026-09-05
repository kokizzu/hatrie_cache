#!/bin/sh
set -eu

go test ./hat/hatSql -run 'TestConsolidateDifferentialRows' -count=1
