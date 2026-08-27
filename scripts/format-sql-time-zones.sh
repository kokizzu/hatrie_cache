#!/bin/sh
set -eu

gofmt -w ./hat/hatSql/time_zone.go ./hat/hatSql/time_zone_test.go
