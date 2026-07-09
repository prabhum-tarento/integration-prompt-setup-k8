---
applyTo: '**/*.yaml, **/*.yml, **/Dockerfile'
description: 'AKS deployment for this service: manifests, health probes, autoscaling (including KEDA for Kafka/Service Bus consumers), and secrets via Workload Identity.'
---

# Kubernetes (AKS) Deployment

## Scope

This repository — `iis-wms-integrations` — is being modernized from an
IIS-hosted deployment model to Azure Kubernetes Service (AKS). Every
manifest and deployment decision in this file targets **AKS**, not generic
Kubernetes-anywhere or a hybrid IIS/K8s setup. If you're asked to produce an
IIS-specific deployment artifact (a `web.config`, an IIS site binding),
that's outside this file's scope — flag it rather than improvising.

The workload is one API deployment (see
[aspnet-rest-apis.instructions.md](aspnet-rest-apis.instructions.md)) plus
two background consumer deployments — Kafka consumer and Service Bus
consumer (see
[integration-resiliency.instructions.md](integration-resiliency.instructions.md)).
The consumers are Deployments with no exposed Service, scaled by KEDA on
queue/topic depth rather than CPU.

## Pods

- One primary container per Pod (add a sidecar only for a genuine
  cross-cutting concern, e.g., a log/metrics shipper).
- Define `resources.requests` and `resources.limits` for every container.
- Every Pod implements `livenessProbe` and `readinessProbe`. Each of the
  three Deployments (API, Kafka consumer, Service Bus consumer) hosts its
  **own** `/health/live` and `/health/ready` — a Pod's probe can only see
  that Pod's own process, so health is never shared across Deployments.
  The API's is defined in
  [aspnet-rest-apis.instructions.md](aspnet-rest-apis.instructions.md); the
  two consumers' are defined in
  [integration-resiliency.instructions.md](integration-resiliency.instructions.md) §8
  (a `ConsumerHealthCheck` per Kafka consumer, `ServiceBusHealthCheck` for
  the Service Bus consumer), each hosted on a small Kestrel listener inside
  the worker process for exactly this purpose.
- Never deploy a bare Pod — always through a Deployment.

## Deployments

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: inventory-api
  labels:
    app: inventory-api
spec:
  replicas: 3
  selector:
    matchLabels:
      app: inventory-api
  template:
    metadata:
      labels:
        app: inventory-api
    spec:
      serviceAccountName: inventory-api-sa   # bound to AKS Workload Identity
      containers:
        - name: inventory-api
          image: <registry>/inventory-api:<digest>
          ports:
            - containerPort: 8080
          resources:
            requests: { cpu: "250m", memory: "256Mi" }
            limits: { cpu: "1", memory: "512Mi" }
          livenessProbe:
            httpGet: { path: /health/live, port: 8080 }
            initialDelaySeconds: 15
            periodSeconds: 20
          readinessProbe:
            httpGet: { path: /health/ready, port: 8080 }
            initialDelaySeconds: 5
            periodSeconds: 10
          securityContext:
            allowPrivilegeEscalation: false
            readOnlyRootFilesystem: true
            capabilities: { drop: [ALL] }
      securityContext:
        runAsNonRoot: true
        runAsUser: 1000
```

- Pin images by digest (`@sha256:...`) for production rollouts, not just by
  avoiding `:latest` — a mutable tag can change under you between build and
  deploy.
- Configure `strategy.rollingUpdate` (`maxSurge`/`maxUnavailable`) explicitly
  rather than relying on defaults.

The two consumer Deployments follow the same Pod template shape (probes,
`securityContext`, resource limits) but with **no `Service`** and their own
health port, since nothing routes HTTP traffic to them:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: servicebus-consumer
  labels:
    app: servicebus-consumer
spec:
  replicas: 1   # KEDA (see Autoscaling below) manages this after initial rollout
  selector:
    matchLabels:
      app: servicebus-consumer
  template:
    metadata:
      labels:
        app: servicebus-consumer
    spec:
      serviceAccountName: servicebus-consumer-sa   # bound to AKS Workload Identity — see Autoscaling
      containers:
        - name: servicebus-consumer
          image: <registry>/servicebus-consumer:<digest>
          ports:
            - containerPort: 8081   # health endpoint only — nothing routes application traffic here
          resources:
            requests: { cpu: "250m", memory: "256Mi" }
            limits: { cpu: "1", memory: "512Mi" }
          livenessProbe:
            httpGet: { path: /health/live, port: 8081 }
            initialDelaySeconds: 15
            periodSeconds: 20
          readinessProbe:
            httpGet: { path: /health/ready, port: 8081 }
            initialDelaySeconds: 5
            periodSeconds: 10
          securityContext:
            allowPrivilegeEscalation: false
            readOnlyRootFilesystem: true
            capabilities: { drop: [ALL] }
      securityContext:
        runAsNonRoot: true
        runAsUser: 1000
```

The Kafka consumer Deployment (`kafka-consumer`) is identical in shape with
its own `ServiceAccount` (`kafka-consumer-sa`) and health port — omitted
here to avoid repeating the same manifest twice.

### Container image

- Build with .NET's built-in container support
  (`dotnet publish --os linux --arch x64 -p:PublishProfile=DefaultContainer`)
  rather than a hand-written Dockerfile where possible — fewer layers to
  maintain and it stays in sync with the SDK version automatically. If a
  hand-written Dockerfile is needed (e.g., to add a non-.NET dependency),
  use a multi-stage build (`mcr.microsoft.com/dotnet/sdk` to build,
  `mcr.microsoft.com/dotnet/aspnet` or the smaller `-chiseled` variant to
  run) so the runtime image doesn't carry the SDK.
- Run as the non-root user the base image already defines
  (`USER $APP_UID` on the `aspnet` images) — don't add your own `USER root`
  step, which would fight the `runAsNonRoot: true` `securityContext` above.
- One image per workload (API, Kafka consumer, Service Bus consumer) —
  don't build a single image that branches on an environment variable to
  decide which one to run; that couples their deployment lifecycles
  together for no benefit.

## Services and Ingress

- `ClusterIP` for the API's internal service; expose externally through the
  cluster's Ingress controller with TLS termination, not a `LoadBalancer`
  Service directly.
- The Kafka and Service Bus consumer Deployments have **no Service** — they
  don't receive inbound traffic.

## Secrets (mandatory, not advisory)

- Credentials, connection strings, API keys, and certificates **MUST** use
  Kubernetes Secrets or, preferably, be resolved at runtime via **AKS
  Workload Identity → Azure Key Vault** — never a client secret, embedded
  credential, or node-managed identity. This matches
  [engineering-standards.instructions.md](engineering-standards.instructions.md) §6
  and [cosmos-db.instructions.md](cosmos-db.instructions.md) §1.
- **MUST NOT** commit a Secret manifest containing real values to source
  control — base64 is encoding, not encryption. Use a sealed-secrets/SOPS
  approach or the External Secrets Operator pulling from Key Vault; commit
  only the reference, never the value.
- ConfigMaps are for non-sensitive configuration only — they are not
  encrypted at rest.
- Prefer mounting secrets as volumes over environment variables; if an env
  var is used, note why in the PR (env vars are more exposure-prone via
  `/proc`, crash dumps, and child-process inheritance).

## Autoscaling

- **API Deployment**: standard `HorizontalPodAutoscaler` on CPU (and/or a
  custom request-rate metric).
- **Kafka consumer / Service Bus consumer Deployments**: **KEDA**
  `ScaledObject`s, scaling on Kafka consumer lag and Service Bus queue
  length respectively — not CPU, since these workloads are I/O-bound and
  CPU-idle while waiting on messages.

Both consumer Deployments need a `TriggerAuthentication` backed by
Workload Identity so KEDA can query queue depth / consumer lag without a
static credential. **The `keda-operator` Pod is what actually performs
this query** — not the consumer's own Pod — because KEDA's metrics
adapter runs as part of the operator, polling trigger sources for every
`ScaledObject` in the cluster centrally. That means the federated identity
credential (in Microsoft Entra ID) must trust the `keda-operator`
ServiceAccount's OIDC subject, not `servicebus-consumer-sa`/
`kafka-consumer-sa`. Use `identityId` to point at the specific
user-assigned managed identity to use for the query — in practice, the
simplest setup reuses the **same** managed identity each consumer's own
Workload Identity already uses (per the Deployments above), with a
*second* federated credential added to that identity trusting
`keda-operator`'s ServiceAccount subject, so one identity serves both the
app's own Azure SDK calls and KEDA's trigger-source polling:

```yaml
apiVersion: keda.sh/v1alpha1
kind: TriggerAuthentication
metadata:
  name: azure-workload-identity-auth
spec:
  podIdentity:
    provider: azure-workload
    identityId: <client-id-of-the-managed-identity>   # see explanation above —
                                                        # this identity needs a federated
                                                        # credential trusting keda-operator's SA
```

```yaml
apiVersion: keda.sh/v1alpha1
kind: ScaledObject
metadata:
  name: servicebus-consumer-scaler
spec:
  scaleTargetRef:
    name: servicebus-consumer
  minReplicaCount: 1
  maxReplicaCount: 10   # the worst-case replica count — see integration-resiliency.instructions.md §6,
                         # its RU capacity formula multiplies by this number
  triggers:
    - type: azure-servicebus
      metadata:
        queueName: inventory-events
        namespace: <service-bus-namespace>   # the Azure Service Bus namespace name, not a k8s namespace —
                                              # required for Workload Identity auth (no connection string
                                              # to derive it from)
        messageCount: "50"
      authenticationRef:
        name: azure-workload-identity-auth
```

This service's Kafka cluster is a genuine question to settle before writing
the Kafka `ScaledObject`: Workload Identity via `azure-workload` pod
identity only applies if the "Kafka" endpoint is **Azure Event Hubs'
Kafka-compatible surface**. If it's a self-managed Kafka cluster (Confluent,
Strimzi, or similar) authenticating via SASL/SCRAM or mTLS instead, KEDA's
`kafka` scaler needs a *different* `TriggerAuthentication` carrying
`sasl`/`tls` fields (`spec.secretTargetRef` pointing at a Secret with the
SASL username/password or client cert) — not an absent `authenticationRef`,
which would mean no authentication at all. Confirm which one applies to
this repo before deploying either version below:

```yaml
apiVersion: keda.sh/v1alpha1
kind: ScaledObject
metadata:
  name: kafka-consumer-scaler
spec:
  scaleTargetRef:
    name: kafka-consumer
  minReplicaCount: 1
  maxReplicaCount: 10   # coupled to integration-resiliency.instructions.md §6's RU formula, same as above
  triggers:
    - type: kafka
      metadata:
        bootstrapServers: "<namespace>.servicebus.windows.net:9093"   # Event Hubs Kafka endpoint
        consumerGroup: inventory-events-consumer
        topic: inventory-events
        lagThreshold: "100"
      authenticationRef:
        name: azure-workload-identity-auth   # Event Hubs Kafka endpoint via Workload Identity only
```

If the broker is a self-managed cluster instead, replace
`authenticationRef` with a SASL/mTLS `TriggerAuthentication` (KEDA's
`kafka` scaler documents the `sasl`/`tls` field names for that case) —
don't reuse `azure-workload-identity-auth` for a non-Azure-native broker.

The Kafka consumer's Deployment must match `scaleTargetRef.name` above
(`kafka-consumer`) — the same rule applies to the Service Bus consumer.

- Use `VerticalPodAutoscaler` in recommendation-only mode to right-size
  `resources` over time; don't let it auto-apply to a workload with strict
  latency requirements without review.

## Security

- `runAsNonRoot: true`, `allowPrivilegeEscalation: false`,
  `readOnlyRootFilesystem: true`, and `capabilities.drop: [ALL]` on every
  container unless a specific, documented exception is required.
- `NetworkPolicy`: deny-by-default, explicit allow rules between the API,
  consumers, and their dependencies.
- RBAC: least-privilege `Role`/`RoleBinding` per `ServiceAccount`; no
  workload runs under `cluster-admin`.
- Minimal, scanned base images (Trivy/Snyk in CI); no `:latest` tags in any
  environment past local dev.

## Observability

- Application logs to `STDOUT`/`STDERR`; the cluster's logging agent ships
  them to the central sink — the app does not manage its own log shipping.
- Metrics via Prometheus (`kube-state-metrics`, `node-exporter`) plus the
  app's own OpenTelemetry metrics (see
  [engineering-standards.instructions.md](engineering-standards.instructions.md) §6).
- Distributed tracing via OpenTelemetry, correlated end-to-end using the
  correlation ID described in
  [aspnet-rest-apis.instructions.md](aspnet-rest-apis.instructions.md) and
  [integration-resiliency.instructions.md](integration-resiliency.instructions.md).
- Alert on: sustained `429`s from Cosmos, rising Service Bus dead-letter
  count, Kafka consumer lag growth, Pod restarts, and failed readiness
  probes.

## Deployment strategy

- Default: rolling update, tuned `maxSurge`/`maxUnavailable`.
- Use a canary (traffic-split via the Ingress controller or a service mesh)
  for a change to the message-processing logic, since a bad consumer
  version can silently corrupt inventory state before anyone notices —
  don't rely on rolling update alone for that class of change.
- `kubectl rollout undo` for fast rollback; keep the previous image digest
  available and referenced, not just "the previous tag."

## Manifest review checklist

- [ ] `apiVersion`/`kind` correct; `metadata.name` descriptive.
- [ ] `resources.requests`/`limits` set on every container.
- [ ] `livenessProbe`/`readinessProbe` point at the right paths/ports.
- [ ] Secrets (not ConfigMaps) hold sensitive values; no plaintext secret
      committed.
- [ ] `runAsNonRoot`, `readOnlyRootFilesystem`, dropped capabilities set.
- [ ] Image referenced by digest, not `:latest`.
- [ ] `NetworkPolicy` and least-privilege RBAC in place.
- [ ] KEDA `ScaledObject` present for the two consumer Deployments; HPA for
      the API.
- [ ] Rolling update strategy explicit, not left as the bare default.

## Troubleshooting

- **Pending/CrashLoopBackOff**: `kubectl describe pod`, check resource
  requests against node capacity, check image pull auth.
- **NotReady**: check the readiness probe path against the app's actual
  health endpoint and its dependency checks (§ Pods above).
- **Service unreachable**: verify `selector`/label match, check
  `NetworkPolicy`, check Ingress controller logs.
- **OOMKilled**: raise `memory.limits` or fix the leak; don't just raise the
  limit repeatedly without profiling.
- **Consumer lag climbing despite KEDA scaling**: check `maxReplicaCount`
  isn't the ceiling, and check the downstream Cosmos DB `429` rate — the
  consumer may be scaled correctly but throttled at the database.
