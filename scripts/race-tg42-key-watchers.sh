#!/bin/sh
set -eu

go test -race ./hat/hatCache -count=1
