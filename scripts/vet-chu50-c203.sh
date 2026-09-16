#!/bin/sh
set -eu

go vet ./hat/hatBackup ./hat/hatCache ./hat/hatSnapshot
