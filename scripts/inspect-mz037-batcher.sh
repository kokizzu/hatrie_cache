#!/bin/sh
set -eu

sed -n '1,430p' hat/hatPipeline/async_batcher.go
sed -n '1,240p' hat/hatPipeline/partitioned_async_batcher.go
