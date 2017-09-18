#!/bin/bash

find_files() {
      find . -not \( \
      \( \
        -wholename '*/vendor/*' \
      \) -prune \
    \) -name '*.go'
}

diff=$(find_files | xargs gofmt -l -s 2>&1) || true
if [[ -n "${diff}" ]]; then
  echo "Following files are not formatted. Please run gofmt -s -w"
  echo "${diff}"
  exit 1
fi
echo "All files are gofmt'ed"
