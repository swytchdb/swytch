# Swytch operator

Run Swytch next to your application or behind a Kubernetes Service. One namespaced
`Swytch` resource defines the deployment, memory limit, and DNS or Swytch Cloud
configuration. A Pod annotation selects a sidecar profile.

Swytch uses subscription-based replication. Servers receive data through client
usage; deploying server replicas does not make them a populated backing tier for
sidecars. DNS supplies peer discovery. Cloud supplies discovery and durability.

Requires Kubernetes **1.33+**. This version supports the Redis interface and uses
native sidecars, allowing application Jobs to finish without a lingering sidecar.

## Install

Swytch releases publish `ghcr.io/swytchdb/swytch-operator:VERSION` for Linux amd64
and arm64, using the release version without the leading `v`. Stable releases also
update `latest`; prereleases only publish their version tag. The images use the
same signing process as the main Swytch image.

From a released checkout, install the chart with its matching operator image:

```sh
helm install swytch ./operator/charts/swytch-operator \
  --namespace swytch-system --create-namespace --wait
```

For an unreleased checkout or a custom build, build and push the independent
operator module to a registry your cluster can access:

```sh
cd operator
docker build -t YOUR_REGISTRY/swytch-operator:0.1.0 .
docker push YOUR_REGISTRY/swytch-operator:0.1.0
helm install swytch ./charts/swytch-operator \
  --namespace swytch-system --create-namespace \
  --set image.repository=YOUR_REGISTRY/swytch-operator \
  --set image.tag=0.1.0 --wait
```

Install one operator per cluster. The chart includes its CRD, RBAC, webhook,
Service, and generated TLS certificates; cert-manager is not required. The
operator watches all namespaces. The webhook only handles annotated Pod creates
and fails closed for those requests if injection is unavailable. Unannotated Pods,
including the operator itself, do not depend on the webhook.

For private registries, set `imagePullSecrets` to a list of `{name: ...}` objects
for the operator. `swytch.imagePullSecrets` is a list of Secret names added to
generated workloads; those Secrets must exist in each application's namespace.

## Sidecar

Create the credential Secret in your application's namespace. Generate a DNS
passphrase with `swytch gen-passphrase`, then store it under `cluster-passphrase`
in a Secret named `swytch-secret`.

```yaml
apiVersion: swytch.getswytch.com/v1alpha1
kind: Swytch
metadata:
  name: app-cache
spec:
  deployment: sidecar
  memory: 512Mi
  sidecar:
    transport: tcp
  discovery:
    dns:
      secretKeyRef:
        name: swytch-secret
        key: cluster-passphrase
```

Add this annotation to the application's **Pod template**, not the Deployment's
top-level metadata:

```yaml
spec:
  template:
    metadata:
      annotations:
        swytch.getswytch.com/profile: app-cache
```

The profile must exist in the same namespace. Applications connect to
`127.0.0.1:6379`. No application environment variables are changed. See
[the complete DNS example](examples/sidecar-dns.yaml).

For Unix sockets, set `sidecar.transport: unix`. The operator mounts an `emptyDir`
at `/var/run/swytch` in every regular application container and the Swytch sidecar.
Applications use `/var/run/swytch/swytch.sock`; Redis TCP is disabled. Socket
permissions allow applications with different UIDs in the same Pod to connect.
The socket is not mounted into application init containers.

Each Pod can reference one profile. Container name `swytch`, the injected metadata
keys, and socket volume name `swytch-socket` are reserved. Declared port or mount
collisions are rejected. Undeclared listeners cannot be detected at admission;
reserve TCP 9090, UDP 7379, and TCP 6379 when using TCP transport. `hostNetwork`
Pods are not supported.

## Server

```yaml
apiVersion: swytch.getswytch.com/v1alpha1
kind: Swytch
metadata:
  name: shared-cache
spec:
  deployment: server
  memory: 1Gi
  server:
    replicas: 2
  discovery:
    dns:
      secretKeyRef:
        name: swytch-secret
        key: cluster-passphrase
```

Applications explicitly connect to `swytch-shared-cache:6379` in the same namespace,
or `swytch-shared-cache.NAMESPACE.svc:6379` from another namespace. The operator
creates a Deployment and ClusterIP Service. Replicas default to one; zero is valid.
Server profiles cannot be referenced by a sidecar annotation. Deployment mode is
immutable; create another profile to change it.

## Discovery and credentials

DNS profiles get a headless Service named `swytch-PROFILE-peers`. It publishes Pod
addresses before readiness so peers can bootstrap. Each process advertises its Pod
IP on UDP 7379. Allow peer traffic between participating Pods in your network
configuration. Peer communication uses the passphrase-derived mTLS identity;
the Redis client Service itself is plain TCP.

For Cloud, replace the DNS block with:

```yaml
discovery:
  cloud:
    secretKeyRef:
      name: swytch-cloud
      key: connection-secret
```

Generate credentials using `swytch gen-passphrase --cloud`. Supply the **connection
secret** to Kubernetes, not the derived Cloud secret used for onboarding. Cloud
configuration replaces both DNS discovery and the cluster passphrase; exactly one
discovery mode is required. The operator does not create Cloud accounts or Secrets.
Secrets are referenced through container environment variables; their contents
are not copied into CRs, generated Pod arguments, or status by the operator.

Examples: [Unix sidecar with Cloud](examples/sidecar-unix-cloud.yaml),
[DNS server](examples/server-dns.yaml), [Cloud server](examples/server-cloud.yaml).

## Sizing and lifecycle

`spec.memory` sets the container memory **limit**. Swytch runs with
`--maxmemory=80%`. The operator omits resource requests unless you configure them:

```yaml
memory: 1Gi
requests:
  memory: 128Mi
  cpu: 100m
limits:
  cpu: "2"
```

Kubernetes may default an omitted request to its corresponding limit, and
namespace LimitRanges or other admission policies may also supply defaults.
Specify requests explicitly when you need control over scheduling reservations.
Requests must not exceed their corresponding limits.

`spec.image` overrides the chart's `swytch.image` default. Changing a server
profile reconciles its Deployment. Sidecar configuration changes apply only to
new Pods; restart application workloads when ready. Secret changes also require
Pod recreation. No existing application Pods are restarted automatically.

`status.observedGeneration` and the `Ready` condition describe configuration
reconciliation and server Deployment availability. `/health` probes report process
health, not data presence or subscription availability. Metrics are available at
Pod port 9090 under `/metrics`.

Deleting a profile garbage-collects its owned server Deployment and Services.
Application Pods remain owned by their applications, and already injected sidecars
continue running. Remove annotations and recreate those Pods when removing a
sidecar profile. DNS peers retain no guarantee of data after all nodes stop; this
operator provisions no PVCs.

## Upgrades and webhook certificates

Before creating a release tag, manually update the chart's `version` and
`appVersion` in `Chart.yaml`, and `image.tag` and `swytch.image` in `values.yaml`
to match the intended release. Commit those changes before tagging. The release
pipeline builds both images from that tag; it does not edit or commit versions.

Helm installs CRDs from `crds/` only on initial installation. Before an operator
upgrade, apply the new CRD explicitly, then upgrade the chart:

```sh
kubectl apply -f charts/swytch-operator/crds/
helm upgrade swytch ./charts/swytch-operator -n swytch-system --reuse-values
```

The chart generates a ten-year CA and a one-year serving certificate. Normal Helm
upgrades reuse them using `lookup`. Before the serving certificate expires,
increment `webhook.certificateRevision`:

```sh
helm upgrade swytch ./charts/swytch-operator -n swytch-system --reuse-values \
  --set-string webhook.certificateRevision=2 --wait
```

This renews the serving certificate with the same CA and rolls the operator.
The Secret stores the CA key for subsequent Helm renewal; only serving certificate
files are mounted into the operator. The operator has no Secret-read RBAC.
Certificate renewal is manual; schedule it before expiry. CA replacement requires
a planned reinstall before its ten-year expiry.

Use Helm against the cluster for upgrades. Offline `helm template` cannot look up
existing certificates and generates fresh ones each time; repeatedly applying
offline renders is not a supported renewal workflow. Do not delete the certificate
Secret while the operator is installed.

## Development and validation

```sh
go build -o /tmp/swytch-operator ./cmd/operator
go test -race ./...
helm lint charts/swytch-operator --kube-version 1.33.0

go install sigs.k8s.io/controller-runtime/tools/setup-envtest@release-0.21
export KUBEBUILDER_ASSETS="$(setup-envtest use 1.33.x -p path)"
go test -v ./test/integration

go install sigs.k8s.io/kind@v0.29.0
bash test/e2e/run.sh
```

API integration tests use a disposable API server and validate the CRD, admission,
reconciliation defaults, and Helm certificate reuse/rotation. The end-to-end script
requires Docker, kind, kubectl, and Helm; it creates its own kubeconfig and cluster,
tests TCP, Unix sockets, server access, subscription-driven peer reads, Job
completion, and certificate renewal, then deletes that cluster. It never uses your
current Kubernetes context. Cloud argument generation is tested locally; actual
Cloud connectivity requires separate testing with provisioned credentials.
