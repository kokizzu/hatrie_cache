#!/bin/sh
set -eu

go test ./hat/hatPipeline -run '^TestMU01Connector' -count=1 -v
