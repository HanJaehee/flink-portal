# Flink Portal

Automatically discover Flink clusters and access their existing Web UIs through
one portal. No per-cluster Ingress, ExternalName Service, or URL registration.

Built with Kotlin, Spring Boot MVC, and the Fabric8 Kubernetes client.

## What it does

- Lists Operator-managed Flink deployments across configured namespaces.
- Matches REST Services by namespace and verified Kubernetes ownership, including
  same-name deployments in different namespaces.
- Proxies Flink dashboards, REST reads, job details, metrics, and log downloads.
- Retains stopped deployments and displays a clear unavailable state.
- Uses read-only Kubernetes access and allows only GET/HEAD through the proxy,
  while blocking legacy GET job termination and uploaded-JAR plan execution.

The portal does not submit, cancel, upgrade, or manage jobs. Use your existing
Operator workflows for lifecycle changes. It does not recreate Flink's dashboard.

## Run locally

Install a Java 21 JDK and configure `JAVA_HOME`. The Gradle wrapper is included.
Your kubeconfig context must have the discovery permissions described below and
`get` on the Kubernetes `services/proxy` subresource.

```sh
./gradlew check bootJar
export KUBECONFIG="$HOME/.kube/config"
export PORTAL_KUBE_CONTEXT=my-context
export PORTAL_NAMESPACES=team-a,team-b
./gradlew bootRun
```

Open <http://127.0.0.1:8080/>. Local mode uses the Kubernetes API's authenticated
Service proxy, so the laptop does not need access to Service/pod CIDRs or a separate
port forward for every cluster. kubeconfig credentials remain on the server.

The discovery loop first reads the configured namespaces, checks ownership and
ready EndpointSlices, then probes the Flink REST endpoint. An initial discovery
may take a few seconds. A missing CRD, forbidden API request, or API connection
failure is shown as an error and returns 503 from `/readyz`.

## Configuration

| Environment variable | Default | Purpose |
| --- | --- | --- |
| `PORTAL_NAMESPACES` | `default` | Comma-separated namespaces; explicit `*` watches all namespaces. |
| `PORTAL_KUBE_CONTEXT` | kubeconfig current context | Local context selection. |
| `KUBECONFIG` | Fabric8 standard lookup | Path to local kubeconfig; not needed in-cluster. |
| `PORTAL_TRANSPORT` | `auto` | `direct`, `api-server`, or `auto`. |
| `PORTAL_BIND_ADDRESS` | `127.0.0.1` | Bind interface; Helm deployment uses `0.0.0.0`. |
| `PORT` | `8080` | HTTP port. |
| `PORTAL_REFRESH_MS` | `5000` | Backend discovery interval in milliseconds. |

`auto` chooses direct Service access when in-cluster credentials are available
(`KUBERNETES_SERVICE_HOST` is set and `KUBECONFIG` is not), otherwise API-server
proxy access. A direct target uses the discovered Service and REST port in its
namespace. API-server TLS and credential handling reuse Fabric8's authenticated
transport, with automatic redirects disabled. Browser Authorization/Cookie
headers are never forwarded to an upstream, and upstream cookies are discarded.

Discovery results expire after 30 seconds; API failures invalidate routing
immediately while the last list remains visible. Each REST probe has a bounded
deadline with at most four probes at once. Streams are passed through in chunks,
with a 30-second request/header timeout and a separate 30-second body idle timeout
that closes stalled responses. Continuously flowing responses are streamed without
whole-body buffering. Servlet requests use Java 21 virtual threads; proxy reads
and writes are synchronous and close the upstream on completion or write failure.
The browser refreshes the list every five seconds independently of the backend interval.

## Deploy in Kubernetes with Helm

The reusable chart is in [`charts/flink-portal`](charts/flink-portal/README.md).
Build and publish your own image, then install in a dedicated namespace. Match
`--platform` to your cluster nodes; the example below targets amd64.

```sh
docker buildx build --platform linux/amd64 \
  -t YOUR_REGISTRY/flink-portal:YOUR_TAG --push .
helm upgrade --install flink-portal charts/flink-portal \
  --namespace flink-portal --create-namespace \
  --set image.repository=YOUR_REGISTRY/flink-portal \
  --set image.tag=YOUR_TAG \
  --set 'portal.namespaces={team-a,team-b}' \
  --wait --timeout 5m
kubectl -n flink-portal port-forward service/flink-portal 18090:8080
```

Open <http://127.0.0.1:18090/>. Target namespaces must already exist. Helm creates
read-only Roles/RoleBindings in each configured namespace for the Portal
ServiceAccount and explicitly selects direct internal Flink Service access.
Provide `imagePullSecrets` for a private image. Registry credentials remain in
separately provisioned Kubernetes Secrets.

The chart's default image path is a placeholder destination, not a published
image. The ClusterIP Service is the single entry point. Enable the optional
root-path Ingress for access through your controller; see the chart README for
values, TLS and cleanup. No per-Flink Ingress or ExternalName Service is needed.

### Access boundary

The portal does not provide built-in login or per-user Kubernetes authorization.
Every person who can reach an instance can read the namespaces that instance is
configured and permitted to expose. Use your existing ingress authentication,
network controls, or a local port forward. Flink's UI/API can reveal job config,
logs, and data; namespace selection is an instance-wide visibility boundary.

Discovery permissions are `get/list/watch` on:

- `flink.apache.org/flinkdeployments`
- core `services` and `pods`
- `apps/deployments`
- `discovery.k8s.io/endpointslices`

The normal Helm direct-mode deployment has no `services/proxy` permission. For local
or explicit `api-server` mode, additionally grant `get` on `services/proxy` in each
configured namespace. Granting `*` discovery scope requires appropriately scoped
ClusterRole bindings; it is not implied by the namespaced chart.

## Availability and troubleshooting

| State | Meaning |
| --- | --- |
| Available | Verified Service ownership, ready EndpointSlice and reachable REST API. |
| Suspended | The CR is suspended and its REST endpoint is unavailable. |
| Starting / Waiting for endpoints | Service or ready endpoints are not present yet. |
| Temporarily unavailable | REST endpoint did not respond successfully to a probe. |
| Service conflict | More than one eligible non-conventional REST Service was found. |
| Discovery unavailable | Kubernetes API discovery failed or its snapshot expired. |

The portal reads `/jobs/overview` once per cluster during each refresh to obtain
availability and all job IDs, names and current states together. This includes
all jobs retained by an application or session cluster, with job-name/ID search.
An empty jobs array means a healthy cluster with no jobs. Reads are bounded to
2.5 seconds and 4 MiB; failed reads or expired discovery display status as
unavailable instead of reusing old job states. Operator state is a separately
labeled fallback when live status is unavailable.

A failed Flink job can still have a working JobManager UI, so job state and UI
availability are shown separately. An offline cluster URL returns an explanatory
503 page. Deleting the CR removes it on the next successful refresh. A CR with
the same name but a new UID cannot inherit the previous deployment's Service.

For connection failures, check your kubeconfig context and `services/proxy`
permission in local mode, or RBAC and Service connectivity in direct mode. For
missing clusters, check namespace selection, the Operator CRD and the resource's
ownership chain. Supported REST Services have native Flink labels and a named
`rest` port; arbitrary standalone services and ExternalName targets are excluded.

Encoded path traversal and encoded separators are rejected. Job mutation methods
are intentionally rejected with HTTP 405. Flink's legacy GET `yarn-cancel` and
`yarn-stop` actions, and GET `jars/<id>/plan` (which runs a JAR entrypoint to produce
its plan), return HTTP 403, including versioned and percent-encoded equivalents.
Portal URLs end with `/` so Flink's
relative resources resolve under the cluster prefix. Expected offline requests
do not generate per-request warning logs.

## Development

```sh
./gradlew test
./gradlew formatKotlin
./gradlew check bootJar
```

Tests cover namespace isolation, ownership and UID replacement, EndpointSlice
readiness, API errors/pagination, snapshot expiry/concurrent reads, proxy header
filtering, redirect handling, credentials, streaming cancellation, and API output.
Real-cluster acceptance uses Flink Kubernetes Operator 1.16.1 and Flink 2.3.0.
Older Flink UI versions have not been verified.

Personal kubeconfig files, demo deployment manifests and local verification
settings are intentionally excluded from this repository.

## Displayed metadata

The image and per-instance JobManager/TaskManager CPU and memory requests/limits
come from `FlinkDeployment.spec`. Legacy singular `resource` fields and flat CPU/memory fields are shown as
requests. These are configured resources, not measured utilization. Missing
values display a dash. Resource quantities retain their configured units.

JM count uses ready replicas on the validated owner Deployment. TM count uses
ready, nonterminating native Flink Pods owned by that Deployment, filtered by
namespace, UID and labels. Counts and resource settings apply to the whole
cluster; session jobs share them. Pod reads require the read-only `pods`
permission included in the Helm chart.

Each job shows its REST `start-time`, formatted with a timezone. Optional job
REST details supply actual vertex parallelism, displayed as one value or a
min–max range when vertices differ. At most four detail requests run concurrently
within the existing 2.5-second status budget, with 4 MiB per response. Failure or
timeout of optional details leaves summary status available and parallelism
unknown. No previous details are reused as current information.
