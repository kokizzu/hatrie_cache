#!/bin/sh
set -eu

go test -race ./hat/hatBackup ./hat/hatCache ./hat/hatSnapshot -count=1
