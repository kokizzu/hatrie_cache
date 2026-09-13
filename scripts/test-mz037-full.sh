#!/bin/sh
set -eu

go test ./hat/hatPipeline ./hat/hatSql ./hat/hatSchema ./hat/hatCache
