#!/bin/bash

find_files() {
      find . -not \( \
      \( \
        -wholename '*/vendor/*' \
      \) -prune \
    \) -name '*.go'
}

find_files | xargs gofmt -s -w

echo "All files are gofmt'ed."
