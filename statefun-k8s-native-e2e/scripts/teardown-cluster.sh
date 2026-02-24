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

CLUSTER_NAME=${1:-statefun-e2e}

if kind get clusters 2>/dev/null | grep -q "^${CLUSTER_NAME}$"; then
  echo "=== Deleting kind cluster: ${CLUSTER_NAME} ==="
  kind delete cluster --name "${CLUSTER_NAME}"
  echo "=== Cluster ${CLUSTER_NAME} deleted ==="
else
  echo "=== Kind cluster ${CLUSTER_NAME} does not exist, nothing to delete ==="
fi
