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

set -euo pipefail

BASEDIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null && pwd)"
PROJECT_ROOT="${BASEDIR}/../.."
E2E_MODULE="${BASEDIR}/.."

echo "=== Building flink-statefun:e2e Docker image ==="
bash "${PROJECT_ROOT}/tools/docker/build-stateful-functions.sh" e2e

echo "=== Building remote function uber JAR ==="
mvn package -f "${E2E_MODULE}/remote-function/pom.xml" -DskipTests -B

echo "=== Building statefun-remote-function:e2e Docker image ==="
docker build -t statefun-remote-function:e2e "${E2E_MODULE}/remote-function"

echo ""
echo "=== All E2E images built ==="
docker images | grep -E "flink-statefun|statefun-remote-function" | head -5
