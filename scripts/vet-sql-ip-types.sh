#!/usr/bin/env bash
set -eu

go vet ./hat/hatSql ./hat/hatSchema ./hat/hatCache
