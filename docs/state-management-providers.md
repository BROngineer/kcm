# State management providers

How kcm deploys the services of a ServiceSet through an external state
management provider, how providers register with kcm, and how to upgrade from
the in-tree ProjectSveltos adapter.

Related documents:

- [`ksm-provider-api` contract](https://github.com/brongineer/ksm-provider-api/blob/main/docs/api.md)
- [`ksm-provider-api` kcm integration guide](https://github.com/brongineer/ksm-provider-api/blob/main/docs/kcm-integration.md)
- [`ksm-provider-sveltos` operations](https://github.com/brongineer/ksm-provider-sveltos/blob/main/docs/operations.md)

---

## 1. Components

| Component | Code | Role |
|---|---|---|
| ServiceSet controller | `internal/controller/serviceset` | reconciles ServiceSets: asks the provider to deploy or delete services, writes the status, verifies service health |
| Provider registry | `internal/ksm/registry.go` | HTTP endpoint adapters register with and heartbeat to; kept in memory |
| Provider clients | `internal/ksm/clients.go` | one client per registered provider, rebuilt when its endpoint, CA or timeout changes |
| Adapter | [`ksm-provider-sveltos`](https://github.com/brongineer/ksm-provider-sveltos) | separate workload translating ServiceSets into ProjectSveltos objects; shipped with the `projectsveltos` component |

kcm no longer contains provider-specific code: the ServiceSet controller talks to
whichever adapter registered under `ServiceSet.spec.provider.name`.

## 2. Reconciling a ServiceSet

1. The controller looks the provider up in the registry. Not registered or
   suspended: it records `status.provider` (with the
   `StateManagementProviderNotReady` / `StateManagementProviderSuspended` events
   on transition) and stops until the provider registers or resumes.
2. It calls the provider's `deployServices` with the ServiceSet's
   name, namespace and generation. A result for an older generation is
   discarded and the reconcile retried.
3. It records the result:

   | Status | Source |
   |---|---|
   | condition `ProviderApplied` | `applied`; when false, the provider's reason (`InvalidInput`, `SourceUnavailable`, `ProviderError`, `TargetUnreachable`) and message |
   | condition `ServiceSetStatusesCollected` | `collected` |
   | `status.services` | the provider's services, except the fields kcm owns: `lastDeployedHash`, the per-service conditions and, for Helm services, `version` |
   | `status.deployed` | all services `Deployed`, and the target cluster exists |

   A false `applied` or `collected` fails the reconcile so it is retried.
4. The health verifier checks Helm services on the target cluster and stamps
   `lastDeployedHash` and `version` when the provider's `deployedHash` advances
   and both the provider and the health rules agree the service is deployed.

On deletion the controller marks the services `Deleting`, calls the provider's
`deleteServices` until it reports done, then removes the finalizer.

Adapters emit `ProviderStateChanged` Events on a ServiceSet when its
provider-side state may have changed; kcm reconciles that ServiceSet. Registry
transitions (registered, suspended, resumed, expired) reconcile all ServiceSets
of the provider.

## 3. Registering a provider

kcm accepts heartbeats for a provider name only from the ServiceAccount declared
by the StateManagementProvider of that name:

```yaml
apiVersion: k0rdent.mirantis.com/v1beta1
kind: StateManagementProvider
metadata:
  name: ksm-projectsveltos
spec:
  registration:
    serviceAccount:
      namespace: projectsveltos
      name: ksm-provider-sveltos
```

- The registry reads the object from kcm's cache on every heartbeat: creating,
  changing or deleting it takes effect without restarting kcm. A rejected
  adapter receives `403 forbidden`.
- `spec.registration` is the only field kcm uses. `selector`, `adapter`,
  `provisioner`, `provisionerCRDs` and `suspend` are kept for compatibility and
  ignored; `selector` labels are still copied onto new ServiceSets.
- Providers usually ship their StateManagementProvider in their chart, so adding
  a provider through the Management or Release objects needs no kcm change.
- Whoever can create or update StateManagementProviders decides which
  ServiceAccounts may register providers. Writing them is part of the
  global-admin role.

## 4. The ProjectSveltos adapter

kcm installs `ksm-provider-sveltos` as a dependency of the `projectsveltos`
component and configures it (`internal/controller/components/values.go`):

| Adapter value | Set to |
|---|---|
| `enabled` | true for the management cluster; false in regional clusters, where Sveltos runs without the adapter |
| `fullnameOverride` | `ksm-provider-sveltos`, the name its StateManagementProvider declares |
| `stateManagementProvider.create` | true: the adapter chart renders `ksm-projectsveltos` |
| `registry.url` | kcm's registry Service URL (`--ksm-registry-url`, rendered by kcm's chart) |
| `systemNamespace`, `kcmServiceAccount` | kcm's namespace and ServiceAccount |
| `tls.insecure` | `ksm.registry.insecure` |

## 5. Configuration

| kcm chart value | Flag | Default | Meaning |
|---|---|---|---|
| `ksm.registry.port` | `--ksm-registry-bind-address` | `8443` | registry port; the Service is `<release>-ksm-registry` |
| `ksm.registry.insecure` | `--ksm-registry-insecure` | `true` | plain HTTP between kcm and adapters, no certificates; **testing and demos only** |
| `ksm.maxProviderTimeout` | `--ksm-max-provider-timeout` | `30s` | upper bound for one provider call |
| — | `--ksm-registry-url` | rendered by the chart | registry URL handed to the adapter kcm installs |
| — | `--ksm-registry-cert-dir` | `/etc/ksm-registry/tls` | serving certificate when TLS is on |
| — | `--ksm-provider-token-file` | `/var/run/secrets/ksm/provider/token` | projected token (audience `ksm-provider`) kcm presents to adapters |

With `ksm.registry.insecure: false` the chart issues the registry certificate
from a dedicated CA through cert-manager. Adapters verify the registry with the
CA Secret `<release>-ksm-registry-ca` (`ca.crt`), copied into their namespace;
the CA outlives renewals of the serving certificate.

kcm authenticates adapters with TokenReview (audience `ksm-registry`) and needs
`create` on `tokenreviews.authentication.k8s.io`.

## 6. Suspending a provider

Scale the adapter Deployment to zero. Its last heartbeat marks the provider
suspended: kcm stops calling it and ServiceSets report `status.provider.suspended`.
Scaling it back up resumes the provider. `StateManagementProvider.spec.suspend`
has no effect.

## 7. Upgrading from the in-tree adapter

The in-tree ProjectSveltos adapter is removed; ServiceSets, Profiles and the
services they deploy are taken over by `ksm-provider-sveltos` without changes.

- **Removed flags:** `--enable-sveltos-ctrl`, `--enable-sveltos-expire-ctrl`. The
  SveltosCluster token renewal moved to the adapter
  (`enableSveltosExpireCtrl` in its chart).
- **Removed controller:** the StateManagementProvider controller. Nothing reads
  StateManagementProvider status anymore.
- **Handover of `ksm-projectsveltos`.** kcm used to create this object. On its
  first reconcile after the upgrade, the Management controller hands it over to
  Helm before the `projectsveltos` component is upgraded: it adds Helm's
  ownership label and annotations, removes the Management owner reference and
  reduces the spec to `registration`. A failure stops the Management reconcile,
  so Helm never upgrades onto an object it cannot adopt. The adapter keeps
  registering throughout.
- **Leftovers to delete by hand.** The removed controller created RBAC for the
  old adapter, which nothing uses now:

  ```sh
  kubectl -n <kcm namespace> delete serviceaccount ksm-projectsveltos-sa
  kubectl delete clusterrole ksm-projectsveltos-cr
  kubectl delete clusterrolebinding ksm-projectsveltos-crb
  ```

- **RBAC:** kcm no longer creates or impersonates ServiceAccounts nor requests
  their tokens.

## 8. Troubleshooting

| Symptom | Check |
|---|---|
| ServiceSets report `provider.ready: false` | the adapter is not registered: its logs show `heartbeat failed`; `403` → no StateManagementProvider named like the provider, or it declares another ServiceAccount; `401` → wrong token audience |
| `provider.suspended: true` | the adapter Deployment is scaled to zero |
| `ProviderApplied` is false | the reason and message come from the adapter, e.g. `SourceUnavailable` for a missing chart source |
| Services stay `Provisioning` | the health rules do not pass on the target cluster yet; see the `ServiceHealth*` conditions of the service |
| Management reports `StateManagementProviderHandOverFailed` | kcm could not update `ksm-projectsveltos`; the event carries the API error |
