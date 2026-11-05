# Flink Portal Helm chart

This chart deploys Portal in the Helm release namespace. It creates a dedicated
ServiceAccount there, then creates a Role/RoleBinding in each explicitly configured
**existing** discovery namespace. The installer needs permission to create those
bindings. No ClusterRole or services/proxy grant is created.

Portal uses its mounted SA token to list Kubernetes resources. Flink REST and UI
traffic goes from Portal to the discovered internal Service DNS; the browser only
connects to Portal. Flink Operator CRDs and native Flink deployments must exist.

## Install

The default public image is [`hindsighta/flink-portal:0.1.0`](https://hub.docker.com/r/hindsighta/flink-portal),
available for linux/amd64 and linux/arm64. No image pull Secret is required for it.
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
