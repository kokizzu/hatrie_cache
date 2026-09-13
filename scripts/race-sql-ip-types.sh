#!/usr/bin/env bash
set -eu

go test -race ./hat/hatSql ./hat/hatSchema ./hat/hatCache
