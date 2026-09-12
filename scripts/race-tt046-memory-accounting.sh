#!/bin/sh
set -eu

go test -race ./hat/hatCache -run 'TestMemoryAccounting|TestMonitoringMemory' -count=1
