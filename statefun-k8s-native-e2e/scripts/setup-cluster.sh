#!/bin/bash
# SPDX-License-Identifier: Apache-2.0


# -----------------------------------------------------------------------------
# Provisions a kind cluster with the full StateFun E2E stack:
#   - cert-manager + Flink Kubernetes Operator
#   - Kafka (single broker, dual listener)
#   - LocalStack (Kinesis + S3)
#   - Remote function HTTP server
#   - FlinkDeployment CR (RocksDB + S3 checkpoints)
#
# Idempotent: deletes any existing cluster with the same name first.
# -----------------------------------------------------------------------------

set -euo pipefail

CLUSTER_NAME=${1:-statefun-e2e}
NAMESPACE=statefun-e2e
FLINK_OPERATOR_VERSION=1.11.0
KAFKA_TOPICS=(counter.commands counter.results greeter.commands greeter.results)
KINESIS_STREAMS=(counter.commands counter.results)
S3_BUCKET=statefun-checkpoints

IMAGE_REGISTRY_PREFIX="${IMAGE_REGISTRY_PREFIX:-}"

BASEDIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null && pwd)"
K8S_MANIFESTS="${BASEDIR}/../src/test/resources/k8s"

# --- Platform-aware tool installation ---------------------------------------

platform() {
  case "$(uname -s)" in
    Linux*)  echo linux ;;
    Darwin*) echo darwin ;;
    MINGW*|MSYS*|CYGWIN*) echo windows ;;
    *)       echo linux ;;
  esac
}

ensure_tool() {
  local tool=$1 linux_url=$2
  command -v "${tool}" >/dev/null 2>&1 && return
  echo "=== Installing ${tool} ==="
  if [[ "$(platform)" == windows ]]; then
    command -v scoop >/dev/null 2>&1 || { echo "ERROR: install ${tool} manually (no scoop available)"; exit 1; }
    scoop install "${tool}"
  else
    curl -fsSL "${linux_url}" -o "/usr/local/bin/${tool}"
    chmod +x "/usr/local/bin/${tool}"
  fi
}

ensure_tool kubectl "https://dl.k8s.io/release/v1.32.3/bin/$(platform)/amd64/kubectl"
ensure_tool kind    "https://kind.sigs.k8s.io/dl/v0.27.0/kind-$(platform)-amd64"
command -v helm >/dev/null 2>&1 || curl -fsSL https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 | bash

# --- Docker sanity ----------------------------------------------------------

echo "=== Verifying Docker daemon ==="
docker info >/dev/null 2>&1 || { echo "ERROR: Docker daemon is not running"; exit 1; }

# --- kind cluster -----------------------------------------------------------

if kind get clusters 2>/dev/null | grep -q "^${CLUSTER_NAME}$"; then
  echo "=== Deleting stale kind cluster: ${CLUSTER_NAME} ==="
  kind delete cluster --name "${CLUSTER_NAME}"
fi

echo "=== Creating kind cluster: ${CLUSTER_NAME} ==="
kind create cluster --name "${CLUSTER_NAME}" --wait 5m

echo "=== Loading Docker images into kind ==="
kind load docker-image flink-statefun:e2e            --name "${CLUSTER_NAME}"
kind load docker-image statefun-remote-function:e2e  --name "${CLUSTER_NAME}"

# --- cert-manager + Flink Operator ------------------------------------------

echo "=== Installing cert-manager ==="
kubectl apply -f https://github.com/cert-manager/cert-manager/releases/latest/download/cert-manager.yaml
for dep in cert-manager cert-manager-webhook cert-manager-cainjector; do
  kubectl wait --for=condition=Available "deployment/${dep}" -n cert-manager --timeout=120s
done

echo "=== Installing Flink Kubernetes Operator ${FLINK_OPERATOR_VERSION} ==="
helm repo add flink-operator-repo \
  "https://archive.apache.org/dist/flink/flink-kubernetes-operator-${FLINK_OPERATOR_VERSION}/" || true
helm repo update
helm install flink-kubernetes-operator flink-operator-repo/flink-kubernetes-operator \
  --namespace flink-operator --create-namespace --wait --timeout 5m

# --- In-namespace infra -----------------------------------------------------

echo "=== Deploying namespace, RBAC, Kafka, LocalStack, remote function, module ==="
kubectl apply -f "${K8S_MANIFESTS}/namespace.yaml"
kubectl apply -f "${K8S_MANIFESTS}/flink-rbac.yaml"
sed -e "s|\${IMAGE_REGISTRY_PREFIX}|${IMAGE_REGISTRY_PREFIX}|g" \
    "${K8S_MANIFESTS}/kafka.yaml" | kubectl apply -f -
sed -e "s|\${IMAGE_REGISTRY_PREFIX}|${IMAGE_REGISTRY_PREFIX}|g" \
    "${K8S_MANIFESTS}/localstack.yaml" | kubectl apply -f -
kubectl apply -f "${K8S_MANIFESTS}/remote-function.yaml"
kubectl apply -f "${K8S_MANIFESTS}/module-configmap.yaml"

for app in kafka localstack remote-function; do
  echo "=== Waiting for ${app} to be ready ==="
  kubectl wait --for=condition=Ready pod -l "app=${app}" -n "${NAMESPACE}" --timeout=180s
done

# --- Kafka topics -----------------------------------------------------------

echo "=== Creating Kafka topics ==="
KAFKA_POD=$(kubectl get pod -n "${NAMESPACE}" -l app=kafka -o jsonpath='{.items[0].metadata.name}')
for topic in "${KAFKA_TOPICS[@]}"; do
  MSYS_NO_PATHCONV=1 kubectl exec -n "${NAMESPACE}" "${KAFKA_POD}" -- \
    /opt/kafka/bin/kafka-topics.sh --create --if-not-exists \
    --bootstrap-server localhost:9092 \
    --topic "${topic}" --partitions 1 --replication-factor 1
done

# --- Kinesis streams + S3 bucket on LocalStack ------------------------------

LOCALSTACK_POD=$(kubectl get pod -n "${NAMESPACE}" -l app=localstack -o jsonpath='{.items[0].metadata.name}')

echo "=== Creating Kinesis streams ==="
for stream in "${KINESIS_STREAMS[@]}"; do
  MSYS_NO_PATHCONV=1 kubectl exec -n "${NAMESPACE}" "${LOCALSTACK_POD}" -- \
    awslocal kinesis create-stream --stream-name "${stream}" --shard-count 1
done
for stream in "${KINESIS_STREAMS[@]}"; do
  for i in $(seq 1 30); do
    status=$(MSYS_NO_PATHCONV=1 kubectl exec -n "${NAMESPACE}" "${LOCALSTACK_POD}" -- \
      awslocal kinesis describe-stream-summary --stream-name "${stream}" \
      --query 'StreamDescriptionSummary.StreamStatus' --output text 2>/dev/null || echo PENDING)
    [[ "${status}" == ACTIVE ]] && { echo "  ${stream}: ACTIVE"; break; }
    [[ $i -eq 30 ]] && { echo "ERROR: stream ${stream} not ACTIVE within 60s"; exit 1; }
    sleep 2
  done
done

echo "=== Creating S3 bucket ${S3_BUCKET} ==="
MSYS_NO_PATHCONV=1 kubectl exec -n "${NAMESPACE}" "${LOCALSTACK_POD}" -- \
  awslocal s3 mb "s3://${S3_BUCKET}"

# --- FlinkDeployment --------------------------------------------------------

echo "=== Deploying FlinkDeployment ==="
kubectl apply -f "${K8S_MANIFESTS}/flink-deployment.yaml"

echo "=== Waiting for FlinkDeployment to be READY ==="
for i in $(seq 1 60); do
  status=$(kubectl get flinkdeployment statefun-jobmanager -n "${NAMESPACE}" \
    -o jsonpath='{.status.jobManagerDeploymentStatus}' 2>/dev/null || echo UNKNOWN)
  echo "  [${i}/60] FlinkDeployment: ${status}"
  [[ "${status}" == READY ]] && { echo "FlinkDeployment is READY!"; break; }
  if [[ $i -eq 60 ]]; then
    echo "ERROR: FlinkDeployment did not reach READY within 5 minutes"
    kubectl describe flinkdeployment statefun-jobmanager -n "${NAMESPACE}" || true
    kubectl logs -n "${NAMESPACE}" -l component=jobmanager --tail=50 || true
    exit 1
  fi
  sleep 5
done

echo
echo "=== Cluster ${CLUSTER_NAME} is ready ==="
kubectl get pods -n "${NAMESPACE}"
