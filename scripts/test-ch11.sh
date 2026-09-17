#!/bin/sh
set -eu

go test ./hat/hatCommand ./hat/hatCache -run 'InsertQuorum'
