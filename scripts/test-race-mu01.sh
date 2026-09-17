#!/bin/sh
set -eu

go test -race ./hat/hatPipeline -count=1
