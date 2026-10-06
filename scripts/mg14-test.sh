#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatPipeline -run 'TestM14|ExampleSinkRetry' -count=1
