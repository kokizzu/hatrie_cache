#!/bin/sh
set -eu

go test ./hat/hatBackup ./hat/hatCache ./hat/hatSnapshot -count=1
