#!/bin/sh
set -eu

go test ./hat/hatCache ./hat/hatReplication -count=1
