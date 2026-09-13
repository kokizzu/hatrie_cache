#!/usr/bin/env bash
set -eu

go test ./hat/hatPipeline -run '^TestMZ04'
