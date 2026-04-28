#!/bin/bash
# SPDX-License-Identifier: Apache-2.0


set -euo pipefail

CLUSTER_NAME=${1:-statefun-e2e}

if kind get clusters 2>/dev/null | grep -q "^${CLUSTER_NAME}$"; then
  echo "=== Deleting kind cluster: ${CLUSTER_NAME} ==="
  kind delete cluster --name "${CLUSTER_NAME}"
  echo "=== Cluster ${CLUSTER_NAME} deleted ==="
else
  echo "=== Kind cluster ${CLUSTER_NAME} does not exist, nothing to delete ==="
fi
