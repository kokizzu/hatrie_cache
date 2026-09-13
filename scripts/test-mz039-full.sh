#!/bin/sh
set -eu

go test ./hat/hatSql ./hat/hatPipeline ./hat/hatSchema ./hat/hatCache
