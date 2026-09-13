#!/usr/bin/env bash
set -eu

go test ./hat/hatPipeline -run 'TestMZ008' -count=1
