#!/bin/sh
set -eu

go test -run '^TestMonitoringSourceFrontier' ./hat/hatCache
