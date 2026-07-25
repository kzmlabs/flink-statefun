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
FLINK_OPERATOR_VERSION=1.15.0
KAFKA_TOPICS=(counter.commands counter.results counter.commands.ttl counter.results.ttl greeter.commands greeter.results)
KINESIS_STREAMS=(counter.commands counter.results)
S3_BUCKET=statefun-checkpoints

IMAGE_REGISTRY_PREFIX="${IMAGE_REGISTRY_PREFIX:-}"
LOCALSTACK_NPM_STRICT_SSL="${LOCALSTACK_NPM_STRICT_SSL:-true}"
LOCALSTACK_NODE_TLS_REJECT="${LOCALSTACK_NODE_TLS_REJECT:-1}"

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
    curl -fsSL "${linux_url}" -o "/tmp/${tool}"
    if [[ $EUID -eq 0 ]]; then
      install -m 0755 "/tmp/${tool}" "/usr/local/bin/${tool}"
    else
      sudo install -m 0755 "/tmp/${tool}" "/usr/local/bin/${tool}"
    fi
    rm -f "/tmp/${tool}"
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
KIND_IMAGE_FLAG=()
if [[ -n "${KIND_NODE_IMAGE:-}" ]]; then
  KIND_IMAGE_FLAG=(--image "${KIND_NODE_IMAGE}")
fi
kind create cluster --name "${CLUSTER_NAME}" --wait 5m "${KIND_IMAGE_FLAG[@]}"

echo "=== Loading Docker images into kind ==="
kind load docker-image flink-statefun:e2e            --name "${CLUSTER_NAME}"
kind load docker-image statefun-remote-function:e2e  --name "${CLUSTER_NAME}"

# Pre-load infra images so kubelet doesn't pull on cold cache and overrun the
# 180s readiness wait. Images go through an explicit single-platform
# `docker save` archive: `kind load docker-image` imports with
# --all-platforms, which fails on Docker's containerd image store where the
# multi-arch manifest list references layers that were never pulled.
echo "=== Pre-loading infra images into kind ==="
# Match the kind node's actual architecture (which can differ from the host's
# when KIND_NODE_IMAGE pins a specific platform), fall back to host uname.
NODE_ARCH="$(docker inspect "${CLUSTER_NAME}-control-plane" --format '{{.Architecture}}' 2>/dev/null || uname -m)"
case "${NODE_ARCH}" in
  amd64|x86_64)   PRELOAD_PLATFORM="linux/amd64" ;;
  arm64|aarch64)  PRELOAD_PLATFORM="linux/arm64" ;;
  *)              PRELOAD_PLATFORM="linux/amd64" ;;
esac
PRELOAD_IMAGES=(
  "${IMAGE_REGISTRY_PREFIX}apache/kafka:3.9.0"
  "${IMAGE_REGISTRY_PREFIX}localstack/localstack:4.1"
)
PRELOAD_TMPDIR="$(mktemp -d)"
trap 'rm -rf "${PRELOAD_TMPDIR}"' EXIT
for img in "${PRELOAD_IMAGES[@]}"; do
  docker pull --platform "${PRELOAD_PLATFORM}" "${img}"
  tarball="${PRELOAD_TMPDIR}/$(echo "${img}" | tr '/:' '__').tar"
  # --platform requires a recent engine; classic-store pulls are already
  # single-platform, so plain save is an equivalent fallback there.
  docker save --platform "${PRELOAD_PLATFORM}" "${img}" -o "${tarball}" 2>/dev/null \
    || docker save "${img}" -o "${tarball}"
  kind load image-archive "${tarball}" --name "${CLUSTER_NAME}"
  rm -f "${tarball}"
done

# --- cert-manager + Flink Operator ------------------------------------------

echo "=== Installing cert-manager ==="
kubectl apply -f https://github.com/cert-manager/cert-manager/releases/latest/download/cert-manager.yaml
for dep in cert-manager cert-manager-webhook cert-manager-cainjector; do
  kubectl wait --for=condition=Available "deployment/${dep}" -n cert-manager --timeout=120s
done

echo "=== Installing Flink Kubernetes Operator ${FLINK_OPERATOR_VERSION} ==="
# --force-update overwrites any stale version-specific URL left by a previous run
# (the repo URL embeds the operator version, so a plain `repo add` on a machine
# that already has the repo would silently keep the old version's chart).
helm repo add --force-update flink-operator-repo \
  "https://archive.apache.org/dist/flink/flink-kubernetes-operator-${FLINK_OPERATOR_VERSION}/"
helm repo update flink-operator-repo
# Pin --version so a chart/URL skew fails loudly instead of installing a
# mismatched chart against the requested image.tag.
helm install flink-kubernetes-operator flink-operator-repo/flink-kubernetes-operator \
  --namespace flink-operator --create-namespace --wait --timeout 5m \
  --version "${FLINK_OPERATOR_VERSION}" \
  --set image.tag="${FLINK_OPERATOR_VERSION}"

# --- In-namespace infra -----------------------------------------------------

echo "=== Deploying namespace, RBAC, Kafka, LocalStack, remote function, module ==="
kubectl apply -f "${K8S_MANIFESTS}/namespace.yaml"
kubectl apply -f "${K8S_MANIFESTS}/flink-rbac.yaml"
sed -e "s|\${IMAGE_REGISTRY_PREFIX}|${IMAGE_REGISTRY_PREFIX}|g" \
    "${K8S_MANIFESTS}/kafka.yaml" | kubectl apply -f -
sed -e "s|\${IMAGE_REGISTRY_PREFIX}|${IMAGE_REGISTRY_PREFIX}|g" \
    -e "s|\${LOCALSTACK_NPM_STRICT_SSL}|${LOCALSTACK_NPM_STRICT_SSL}|g" \
    -e "s|\${LOCALSTACK_NODE_TLS_REJECT}|${LOCALSTACK_NODE_TLS_REJECT}|g" \
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

echo "=== Waiting for LocalStack kinesis provider to respond ==="
# Bound each invocation: LocalStack's lazy `kinesis-local` install can stall a
# single `awslocal` call ~70s on networks with proxy/cert issues, making the
# nominal 60x2s loop budget meaningless. 10s gives slow-but-not-hung installs
# a fair first chance while keeping the worst case finite (~10 min).
for i in $(seq 1 60); do
  if MSYS_NO_PATHCONV=1 kubectl exec -n "${NAMESPACE}" "${LOCALSTACK_POD}" -- \
      timeout 10 awslocal kinesis list-streams >/dev/null 2>&1; then
    echo "  LocalStack kinesis is responding"
    break
  fi
  [[ $i -eq 60 ]] && { echo "ERROR: LocalStack kinesis not responding"; exit 1; }
  sleep 2
done

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
