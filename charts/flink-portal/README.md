# Flink Portal Helm chart

This chart deploys Portal in the Helm release namespace. It creates a dedicated
ServiceAccount there, then creates a Role/RoleBinding in each explicitly configured
**existing** discovery namespace. The installer needs permission to create those
bindings. No ClusterRole or services/proxy grant is created.

Portal uses its mounted SA token to list Kubernetes resources. Flink REST and UI
traffic goes from Portal to the discovered internal Service DNS; the browser only
connects to Portal. Flink Operator CRDs and native Flink deployments must exist.

## Install

The default image reference is [`hindsighta/flink-portal:0.3.0`](https://hub.docker.com/r/hindsighta/flink-portal).
The release workflow publishes linux/amd64 and linux/arm64 images publicly.
Wait for publication to finish before installing a new release; no image pull
Secret is required for a published public image.
Override `image.repository` and `image.tag` to use your own build, and provide
`imagePullSecrets` if that image is private.

```sh
helm lint charts/flink-portal --strict
helm upgrade --install flink-portal charts/flink-portal \
  --namespace flink-portal --create-namespace \
  --set 'portal.namespaces={flink-demo-a,flink-demo-b}' \
  --wait --timeout 5m
kubectl -n flink-portal port-forward service/flink-portal 18090:8080
```

Open http://127.0.0.1:18090/. Port-forward only exposes Portal; it does not forward
or expose individual Flink Services. For external access, enable the chart's
**root-path Ingress** and configure authentication at the controller. Portal itself
has no login: every user of an instance can view every configured namespace.

## Ingress

An Ingress controller must already be installed; this chart does not install one.
Configure DNS to resolve your chosen hostname to that controller. For example:

```yaml
ingress:
  enabled: true
  className: traefik
  annotations: {}
  hosts:
    - portal.example.com
  tls:
    - secretName: portal-tls
      hosts:
        - portal.example.com
```

Apply your values with `helm upgrade --install ... -f your-values.yaml`. TLS Secrets
must exist in the release namespace, or be provisioned by your certificate manager.
Use controller-specific annotations for authentication and HTTPS redirects.
For a trusted LAN test without TLS, leave `tls: []` and open `http://YOUR_HOST/`.
Some domains, including `.dev`, require HTTPS in browsers.

Each host routes `/` with `Prefix` matching to the Portal Service, including all
proxied Flink UI/API paths. Subpath hosting and path-rewrite annotations are not
supported. No per-Flink Ingress is needed. `ingress.enabled` defaults to false;
enabling it requires at least one host. `className` is optional for controllers
that use a default class or the `kubernetes.io/ingress.class` annotation instead.

## Important values

- `portal.namespaces`: explicit nonempty, unique list; `*` is intentionally rejected.
- `portal.refreshMs`: refresh delay, default 5000 ms.
- `image.repository`, `image.tag`, `image.pullPolicy`, `imagePullSecrets`.
- `serviceAccount.create/name`: set create=false and name to use an existing SA.
- `rbac.create`: disable if permissions are managed externally.
- `replicaCount`, `resources`, `nodeSelector`, `tolerations`, `affinity`.
- `service.port`: Portal Service port; the container listens on 8080.
- `ingress.enabled`, `ingress.className`, `ingress.annotations`, `ingress.hosts`, `ingress.tls`.

Deployment explicitly selects direct transport. It mounts only a writable /tmp,
runs as nonroot, and uses startup/liveness/readiness probes. Allow traffic from
Portal to the API server, DNS, and each Flink REST Service under your network
policies. CPU/memory settings and namespace access are independent of job writes;
this chart grants no Pod creation/deletion or Flink mutation permissions.

```sh
helm template flink-portal charts/flink-portal --namespace flink-portal \
  --set 'portal.namespaces={flink-demo-a,flink-demo-b}'
helm package charts/flink-portal --destination /tmp
helm uninstall flink-portal --namespace flink-portal
```

Uninstall removes the release's cross-namespace Roles/RoleBindings too. It does
not delete target namespaces, Flink CRDs or jobs. The chart has no dependencies.

## OCI distribution

After a release is published by GitHub Actions and the GHCR package is public,
install the packaged chart using
`oci://ghcr.io/hanjaehee/charts/flink-portal` with `--version <chart-version>`.
The chart version is independent of its `appVersion` and the Docker image tag.
See the repository's [CI and publishing instructions](../../README.md#ci-and-publishing)
for token setup, release tagging, and initial package visibility. Source installs
above remain available before the first OCI publication.

## Operator monitor (optional)

Enable the Operators tab with status, retained Kubernetes events and live Operator logs:

```yaml
portal:
  namespaces: [flink-jobs-a, flink-jobs-b]
  operatorMonitor:
    enabled: true
    namespaces: [flink-operator]
```

Discovery selects Deployments labelled `app.kubernetes.io/name=flink-kubernetes-operator`
inside the configured Operator namespaces. Custom installations must provide that label.
Pods must be owned by a ReplicaSet belonging to the selected Deployment. Resource activity
covers `portal.namespaces`; it does not infer which Operator owns a FlinkDeployment when
multiple Operators watch overlapping namespaces.

When `rbac.create=true`, monitoring adds namespaced Roles for deployment/replicaset/pod
get/list/watch and `pods/log` get in Operator namespaces, plus event reads in discovery namespaces.
No Secrets, exec or write permissions are granted. Kubernetes RBAC grants log reads for
all Pods in those Operator namespaces; Portal additionally restricts its log endpoint to
verified Operator-owned Pods. With `rbac.create=false`, provision equivalent permissions.

Manual snapshots contain the last hour, default 500/max 2,000 lines, up to 256 KiB. Live logs start with the same recent tail and keep following new output, with a rolling 256 KiB browser buffer. They are
read through Portal's ServiceAccount; the browser never contacts Kubernetes directly.
Events expire in Kubernetes, and no history is stored. Lifecycle is the Operator's last
reported observation, not an exact live task or progress percentage. Jobs submitted directly
to a session cluster have no `FlinkSessionJob` activity here. This monitor currently lists
FlinkDeployments only. Kubernetes status changes use shared reconnecting Watches and SSE to the browser. If Watches
are unavailable, the UI labels snapshot fallback instead of claiming to be live. Live logs
can be paused and optionally auto-scrolled. Streams rotate about every two minutes and
reconnect automatically, reloading the recent tail; there is no gap-free archival replay.
Changing containers, leaving the Operators tab or hiding the page closes browser streams.
The server allows 16 status streams and 4 combined snapshot/follow log requests per instance,
with a bounded follow queue. Slow log readers reconnect with a fresh tail. Reverse proxies
must allow SSE and avoid response buffering. Metrics/Prometheus is not required.

Portal has no built-in authentication. Its existing trusted-network or authenticated
Ingress boundary also protects the operational events and logs exposed by this option.
