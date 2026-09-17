#!/bin/sh
set -eu

go test -race ./hat/hatStorage -run '^TestCH015StorageTierMove'
