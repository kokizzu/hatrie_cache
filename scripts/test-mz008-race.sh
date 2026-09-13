#!/usr/bin/env bash
set -eu

go test -race ./hat/hatPipeline -run 'TestMZ008' -count=1
