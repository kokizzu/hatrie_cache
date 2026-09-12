#!/bin/sh
set -eu

go test ./hat/hatCache -run 'TestMemoryAccounting|TestMonitoringMemoryStructuresEndpoint' -count=1
