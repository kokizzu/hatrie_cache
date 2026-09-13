#!/usr/bin/env bash
set -eu

go test -race ./hat/hatPipeline -run 'TestMZ027' -count=1
