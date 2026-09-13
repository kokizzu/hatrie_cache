#!/usr/bin/env bash
set -eu

go test ./hat/hatSql ./hat/hatSchema ./hat/hatCache
