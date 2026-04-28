#!/bin/bash
# SPDX-License-Identifier: Apache-2.0

if ! command -v go &> /dev/null
then
    echo "Could not find go compiler; skipping tests"
    exit 0
fi

CURR_DIR=`pwd`
BASE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"
GO_SDK_DIR="${BASE_DIR}/../statefun-sdk-go/v3/"


cd ${GO_SDK_DIR}
go test ./...
cd ${CURR_DIR}
