#!/usr/bin/env bash
set -eu
go test ./hat/hatSchema -run 'TestTR046SchemaDiscovery' -count=1
