#!/usr/bin/env bash
set -euo pipefail

mode=${1:-plan}
paths=("${PWD}/.go-build-cache" "${PWD}/.go-tmp")

for path in "${paths[@]}"; do
	if [[ -e "${path}" ]]; then
		printf '%s\t%s\n' "${mode^^}" "${path}"
	fi
done

if [[ "${mode}" == "apply" ]]; then
	for path in "${paths[@]}"; do
		if [[ -e "${path}" ]]; then
			rm -rf -- "${path}"
		fi
	done
fi
