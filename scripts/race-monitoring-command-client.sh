#!/bin/sh
set -eu

go test -race ./hat/hatMonitoring -count=1
