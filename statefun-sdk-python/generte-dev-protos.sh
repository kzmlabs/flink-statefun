#!/bin/bash
# SPDX-License-Identifier: Apache-2.0


CURR_DIR=`pwd`
BASE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"
SDK_PROTOS_DIR="${BASE_DIR}/../statefun-sdk-protos/src/main/protobuf"


cd ${BASE_DIR}
find ${SDK_PROTOS_DIR} -type f -name "*proto" -exec cp {} . \;
protoc *proto --python_out=statefun/
rm *proto
cd ${CURR_DIR}
