#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -e

VERSION_TAG=${1:-3.4.0-KZM-2.0-RC5}

#
# setup the environment
#
basedir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"
project_root="${basedir}/../../" # ditch tools/docker

#
# check if the artifacts were built
#
uber_jar=$(find ${project_root} -path "*/statefun-flink-runner/target/statefun-flink-runner*.jar" -not -name "original-*" -not -name "*example*" -not -name "*sources*" -not -name "*javadoc*" | head -1)
if [[ -z "${uber_jar}" ]]; then
	echo "unable to find statefun-flink-runner jar, please build the maven project first"
	exit 1
fi
core_jar=$(find ${project_root} -path "*/statefun-flink-core/target/statefun-flink-core*.jar" -not -name "*javadoc*" -not -name "*sources*" | head -1)
if [[ -z "${core_jar}" ]]; then
	echo "unable to find statefun-flink-core jar, please build the maven project first"
	exit 2
fi
dist_jar=$(find ${project_root} -path "*/statefun-flink-distribution/target/statefun-flink-distribution*.jar" -not -name "original-*" -not -name "*sources*" -not -name "*javadoc*" | head -1)
if [[ -z "${dist_jar}" ]]; then
	echo "unable to find statefun-flink-distribution jar, please build the maven project first"
	exit 3
fi

#
# create a scratch space for a minimal docker context
#
docker_context_root=`mktemp -d 2>/dev/null || mktemp -d -t 'statefun-docker-context'`

#
# prepare the docker context
#
mkdir -p ${docker_context_root}/flink/lib
cp ${core_jar} ${docker_context_root}/flink/lib/statefun-flink-core.jar
cp ${dist_jar} ${docker_context_root}/flink/lib/statefun-flink-distribution.jar
cp ${uber_jar} ${docker_context_root}/statefun.jar

# build the docker image
cd ${docker_context_root}
cp ${basedir}/Dockerfile ${docker_context_root}
docker build . -t flink-statefun:${VERSION_TAG}

# clean again
rm -rf ${docker_context_root}
