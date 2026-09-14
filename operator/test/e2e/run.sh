#!/usr/bin/env bash
# Creates and destroys only its own kind cluster. Never uses the current context.
set -euo pipefail
cd "$(dirname "$0")/../.."
cluster="swytch-operator-test-${RANDOM}"
test_dir="$(mktemp -d)"
export KUBECONFIG="$test_dir/kubeconfig"
cleanup() {
  result=$?
  if [ "$result" -ne 0 ]; then
    kubectl get pods -A -o wide || true
    kubectl get events -A --sort-by=.lastTimestamp || true
    kubectl logs -n swytch-system deployment/test-swytch-operator --all-containers || true
    kubectl logs -l swytch.getswytch.com/profile=app-cache -c swytch --prefix || true
  fi
  kind delete cluster --name "$cluster"
  rm -rf "$test_dir"
  exit "$result"
}
trap cleanup EXIT
kind create cluster --name "$cluster" --image kindest/node:v1.33.1 --kubeconfig "$KUBECONFIG" --wait 120s
docker build -t swytch-operator:e2e .
kind load docker-image swytch-operator:e2e --name "$cluster"
helm install test charts/swytch-operator --namespace swytch-system --create-namespace \
  --set image.repository=swytch-operator --set image.tag=e2e --wait --timeout 120s
kubectl create secret generic swytch-secret --from-literal=cluster-passphrase=operator-test-only-shared-passphrase
kubectl apply -f examples/sidecar-dns.yaml -f examples/server-dns.yaml
kubectl rollout status deployment/example-app --timeout=180s
kubectl rollout status deployment/swytch-shared-cache --timeout=180s
pod_names=()
while IFS= read -r name; do pod_names+=("$name"); done < <(kubectl get pods -l app=example-app -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}')
test "${#pod_names[@]}" -eq 2
test "$(kubectl exec "${pod_names[0]}" -c app -- redis-cli PING)" = PONG
test "$(kubectl exec "${pod_names[0]}" -c app -- redis-cli -h swytch-shared-cache PING)" = PONG
# Demand on both peers is intentional: Swytch data placement is subscription based.
kubectl exec "${pod_names[1]}" -c app -- redis-cli GET operator-test-key
test "$(kubectl exec "${pod_names[0]}" -c app -- redis-cli SET operator-test-key works)" = OK
matched=false
for attempt in $(seq 1 30); do
  if [ "$(kubectl exec "${pod_names[1]}" -c app -- redis-cli GET operator-test-key)" = works ]; then matched=true; break; fi
  sleep 1
done
test "$matched" = true
cat <<'YAML' | kubectl apply -f -
apiVersion: swytch.getswytch.com/v1alpha1
kind: Swytch
metadata:
  name: unix-cache
spec:
  deployment: sidecar
  memory: 512Mi
  sidecar:
    transport: unix
  discovery:
    dns:
      secretKeyRef:
        name: swytch-secret
        key: cluster-passphrase
---
apiVersion: batch/v1
kind: Job
metadata:
  name: unix-client
spec:
  template:
    metadata:
      annotations:
        swytch.getswytch.com/profile: unix-cache
    spec:
      restartPolicy: Never
      containers:
        - name: app
          image: redis:7.4-alpine
          command: [sh, -ec, 'until redis-cli -s /var/run/swytch/swytch.sock PING; do sleep 1; done']
YAML
kubectl wait --for=condition=complete job/unix-client --timeout=180s
helm upgrade test charts/swytch-operator --namespace swytch-system --reuse-values \
  --set-string webhook.certificateRevision=2 --wait --timeout 120s
# New Pod creation must still work after renewal.
kubectl rollout restart deployment/example-app
kubectl rollout status deployment/example-app --timeout=180s
kubectl patch swytch shared-cache --type=merge -p '{"spec":{"server":{"replicas":0}}}'
kubectl wait --for=jsonpath='{.spec.replicas}'=0 deployment/swytch-shared-cache --timeout=30s
echo 'Operator end-to-end checks passed.'
