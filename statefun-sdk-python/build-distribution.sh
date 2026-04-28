#!/bin/bash
# SPDX-License-Identifier: Apache-2.0

CURR_DIR=`pwd`
BASE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"
SDK_PROTOS_DIR="${BASE_DIR}/../statefun-sdk-protos/src/main/protobuf"
IN_DOCKER_BUILD_COMMAND="
apk add protobuf-dev && \
protoc *proto --python_out=statefun/ && \
python3 setup.py sdist bdist_wheel
"


###########################

cd ${BASE_DIR}

# create a target/ directory like in MAVEN.
# this directory will contain a temporary copy of the source and
# addtional generated sources (for example sdk .proto files copied from else where)
# eventually the built artifact will be copied to ${BASE_DIR}/dist and this target
# directory will be deleted.
rm -fr dist/
rm -fr target/
mkdir -p target/

# copy all the sources into target
rsync -a --exclude=target * target/

# copy the additional .proto files from the SDK
find ${SDK_PROTOS_DIR} -type f -name "*proto" -exec cp {} target/ \;

cd target/

# built the Python SDK inside a Docker container.
# This build step also generates Protobuf files.
docker run -v "${BASE_DIR}/target:/app" \
	--rm \
	--workdir /app \
	-i python:3.7-alpine /bin/sh -c "${IN_DOCKER_BUILD_COMMAND}"

cp -r "dist/" "${BASE_DIR}/dist"

echo "Built Python SDK wheels and packages at ${BASE_DIR}/dist."

cd ${CURR_DIR}
