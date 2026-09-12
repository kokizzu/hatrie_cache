#!/bin/sh
set -eu

go test -race ./hat/hatStorage -run 'TestCompactionScheduler' -count=1
