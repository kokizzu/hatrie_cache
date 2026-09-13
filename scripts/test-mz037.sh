#!/bin/sh
set -eu

go test ./hat/hatPipeline -run 'MZ037'
