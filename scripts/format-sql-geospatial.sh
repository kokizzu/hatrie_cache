#!/usr/bin/env sh
set -eu

gofmt -w hat/hatSql/geospatial.go hat/hatSql/geospatial_test.go hat/hatSql/temporal_analytics.go hat/hatSql/external_quality_test.go
