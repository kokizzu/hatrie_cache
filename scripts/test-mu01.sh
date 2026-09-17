#!/bin/sh
set -eu

go test ./hat/hatPipeline -run '^TestMU01' -count=1
