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
NAMESPACE=statefun-e2e
FLINK_OPERATOR_VERSION=1.11.0

BASEDIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null && pwd)"
PROJECT_ROOT="${BASEDIR}/.."
K8S_MANIFESTS="${PROJECT_ROOT}/src/test/resources/k8s"

# --- Platform detection ---

detect_platform() {
  local uname_s
  uname_s=$(uname -s)
  case "${uname_s}" in
    Linux*)   echo "linux" ;;
    Darwin*)  echo "darwin" ;;
    MINGW*|MSYS*|CYGWIN*) echo "windows" ;;
    *)        echo "linux" ;;
  esac
}

PLATFORM=$(detect_platform)

# --- Auto-install missing tools ---

install_with_scoop() {
  local tool=$1
  if command -v scoop >/dev/null 2>&1; then
    echo "Installing ${tool} via scoop..."
    scoop install "${tool}"
  else
    echo "ERROR: ${tool} is not installed and scoop is not available."
    echo "Please install ${tool} manually."
    exit 1
  fi
}

install_linux_tool() {
  local tool=$1 url=$2
  echo "Installing ${tool}..."
  curl -fsSL "${url}" -o "/usr/local/bin/${tool}"
  chmod +x "/usr/local/bin/${tool}"
}

ensure_kubectl() {
  if command -v kubectl >/dev/null 2>&1; then return; fi
  echo "=== Installing kubectl ==="
  if [[ "${PLATFORM}" == "windows" ]]; then install_with_scoop kubectl
  else install_linux_tool kubectl "https://dl.k8s.io/release/v1.32.3/bin/${PLATFORM}/amd64/kubectl"; fi
}

ensure_kind() {
  if command -v kind >/dev/null 2>&1; then return; fi
  echo "=== Installing kind ==="
  if [[ "${PLATFORM}" == "windows" ]]; then install_with_scoop kind
  else install_linux_tool kind "https://kind.sigs.k8s.io/dl/v0.27.0/kind-${PLATFORM}-amd64"; fi
}

ensure_helm() {
  if command -v helm >/dev/null 2>&1; then return; fi
  echo "=== Installing helm ==="
  if [[ "${PLATFORM}" == "windows" ]]; then install_with_scoop helm
  else curl -fsSL https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 | bash; fi
}

ensure_kubectl
ensure_kind
ensure_helm

# --- Verify Docker is running ---

echo "=== Verifying Docker daemon ==="
if ! docker info >/dev/null 2>&1; then
  echo "ERROR: Docker daemon is not running. Please start Docker Desktop and try again."
  exit 1
fi

# --- Create kind cluster (delete stale cluster first) ---

if kind get clusters 2>/dev/null | grep -q "^${CLUSTER_NAME}$"; then
  echo "=== Deleting stale kind cluster: ${CLUSTER_NAME} ==="
  kind delete cluster --name "${CLUSTER_NAME}"
fi

echo "=== Creating kind cluster: ${CLUSTER_NAME} ==="
kind create cluster --name "${CLUSTER_NAME}" --wait 5m

# --- Load Docker images into kind ---

echo "=== Loading Docker images into kind ==="
kind load docker-image flink-statefun:e2e --name "${CLUSTER_NAME}"
kind load docker-image statefun-remote-function:e2e --name "${CLUSTER_NAME}"

# --- Verify API server connectivity ---

echo "=== Waiting for API server to be reachable ==="
for i in $(seq 1 12); do
  if kubectl cluster-info --context "kind-${CLUSTER_NAME}" >/dev/null 2>&1; then
    echo "API server is reachable."
    break
  fi
  if [[ $i -eq 12 ]]; then
    echo "ERROR: API server not reachable after 60s."
    exit 1
  fi
  echo "  [${i}/12] Waiting for API server..."
  sleep 5
done

# --- Install cert-manager ---

echo "=== Installing cert-manager ==="
kubectl apply -f https://github.com/cert-manager/cert-manager/releases/latest/download/cert-manager.yaml
echo "Waiting for cert-manager to be ready..."
kubectl wait --for=condition=Available deployment/cert-manager -n cert-manager --timeout=120s
kubectl wait --for=condition=Available deployment/cert-manager-webhook -n cert-manager --timeout=120s
kubectl wait --for=condition=Available deployment/cert-manager-cainjector -n cert-manager --timeout=120s

# --- Install Flink Kubernetes Operator 1.11 ---

echo "=== Installing Flink Kubernetes Operator ${FLINK_OPERATOR_VERSION} ==="
helm repo add flink-operator-repo \
  "https://archive.apache.org/dist/flink/flink-kubernetes-operator-${FLINK_OPERATOR_VERSION}/" || true
helm repo update
helm install flink-kubernetes-operator flink-operator-repo/flink-kubernetes-operator \
  --namespace flink-operator \
  --create-namespace \
  --wait \
  --timeout 5m

# --- Deploy E2E infrastructure ---

echo "=== Creating namespace ${NAMESPACE} ==="
kubectl apply -f "${K8S_MANIFESTS}/namespace.yaml"

echo "=== Deploying RBAC ==="
kubectl apply -f "${K8S_MANIFESTS}/flink-rbac.yaml"

echo "=== Deploying MinIO ==="
kubectl apply -f "${K8S_MANIFESTS}/minio.yaml"

echo "=== Deploying Kafka ==="
kubectl apply -f "${K8S_MANIFESTS}/kafka.yaml"

echo "=== Deploying remote function ==="
kubectl apply -f "${K8S_MANIFESTS}/remote-function.yaml"

echo "=== Deploying module ConfigMap ==="
kubectl apply -f "${K8S_MANIFESTS}/module-configmap.yaml"

# --- Wait for infra pods ---

echo "=== Waiting for MinIO to be ready ==="
kubectl wait --for=condition=Ready pod -l app=minio -n "${NAMESPACE}" --timeout=180s

echo "=== Waiting for Kafka to be ready ==="
kubectl wait --for=condition=Ready pod -l app=kafka -n "${NAMESPACE}" --timeout=180s

echo "=== Waiting for remote function to be ready ==="
kubectl wait --for=condition=Ready pod -l app=remote-function -n "${NAMESPACE}" --timeout=180s

# --- Pre-create Kafka topics ---

echo "=== Creating Kafka topics ==="
KAFKA_POD=$(kubectl get pod -n "${NAMESPACE}" -l app=kafka -o jsonpath='{.items[0].metadata.name}')
for TOPIC in commands-proto commands-json results-proto results-json; do
  # MSYS_NO_PATHCONV prevents MINGW from mangling Linux paths
  MSYS_NO_PATHCONV=1 kubectl exec -n "${NAMESPACE}" "${KAFKA_POD}" -- \
    /opt/kafka/bin/kafka-topics.sh --create --if-not-exists \
    --bootstrap-server localhost:9092 \
    --topic "${TOPIC}" --partitions 1 --replication-factor 1
done
echo "Kafka topics created"

# --- Deploy FlinkDeployment ---

echo "=== Deploying FlinkDeployment ==="
kubectl apply -f "${K8S_MANIFESTS}/flink-deployment.yaml"

echo "=== Waiting for FlinkDeployment to be RUNNING ==="
for i in $(seq 1 60); do
  STATUS=$(kubectl get flinkdeployment statefun-jobmanager -n "${NAMESPACE}" \
    -o jsonpath='{.status.jobManagerDeploymentStatus}' 2>/dev/null || echo "UNKNOWN")
  echo "  [${i}/60] FlinkDeployment status: ${STATUS}"
  if [[ "${STATUS}" == "READY" ]]; then
    echo "FlinkDeployment is READY!"
    break
  fi
  if [[ $i -eq 60 ]]; then
    echo "ERROR: FlinkDeployment did not reach READY state within 5 minutes"
    echo "=== FlinkDeployment describe ==="
    kubectl describe flinkdeployment statefun-jobmanager -n "${NAMESPACE}" || true
    echo "=== JobManager logs ==="
    kubectl logs -n "${NAMESPACE}" -l component=jobmanager --tail=50 || true
    exit 1
  fi
  sleep 5
done

echo ""
echo "=== Cluster ${CLUSTER_NAME} is ready ==="
kubectl get pods -n "${NAMESPACE}"
