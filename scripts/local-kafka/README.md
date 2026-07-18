# Local Kafka via Podman

Runs a single-node Kafka broker (KRaft mode, no ZooKeeper) and a Schema Registry under
Podman Desktop, both requiring a username/password - the broker via SASL/PLAIN, the
registry via HTTP Basic Auth - mirroring the credentialed shape of `appsettings.json`'s
Confluent Cloud config (`Kafka:Username`/`Password`/`SchemaRegistryApiKey`/
`SchemaRegistryApiSecret`) rather than the unauthenticated
[appsettings.Developments.json](../../src/Api/IIS.WMS.Consumer.Api/appsettings.Developments.json)
profile:

| Setting | Value |
|---|---|
| `Kafka:BootstrapServers` | `localhost:9092` |
| `Kafka:Protocol` | `SaslPlaintext` (SASL, no TLS - there's no cert locally) |
| `Kafka:AuthenticationMode` | `Plain` |
| `Kafka:Username` / `Kafka:Password` | `kafkaclient` / `kafkaclient-secret` (defaults - see script) |
| `Kafka:SchemaRegistryUrl` | `http://localhost:8085` |
| `Kafka:SchemaRegistryApiKey` / `Kafka:SchemaRegistryApiSecret` | `schemaregistry` / `schemaregistry-secret` (defaults) |
| Kafka data volume (`.bat` and `kube play` setups - not the custom-image path) | Named Podman volume `iis-wms-kafka-data`, persists across reruns - see [Cleanup](#cleanup) to reset it |
| Topics | `inventory-events`, `inventory-bulk-import` |
| Consumer groups | `inventory-events-consumer`, `$InventoryStateChanged`, `inventory-bulk-import-consumer` (appsettings.json's actual `Kafka:ConsumerGroup` values) |
| Schema subject | `inventory-events-value` (a trimmed local-test version of the real Avro contract - see [registration/inventory-state-changed.avsc](registration/inventory-state-changed.avsc)) |
| Kafka UI (`.bat` and `kube play` setups - not the custom-image path) | `http://localhost:8090` |
| Kafka REST Proxy, curl-based produce API (`.bat` and `kube play` setups - not the custom-image path) | `http://localhost:8086` |
| `events-api.ps1` wrapper, run manually (see "Producing a real InventoryStateChanged event" below) | `http://localhost:8087` |

All five ports above (`.bat` setup only) are defaults from [../ports.env](../ports.env) - see
[Configuring ports](#configuring-ports) to change any of them.

## Prerequisites

- [Podman Desktop](https://podman-desktop.io/) installed, with its Podman machine started.
- Outbound internet access the first time you run this, to pull
  `confluentinc/cp-kafka:7.6.0`, `confluentinc/cp-schema-registry:7.6.0`,
  `docker.io/kafbat/kafka-ui:v1.5.0`, and `confluentinc/cp-kafka-rest:8.2.2` (deliberately
  newer than the broker/registry - see "Producing a real InventoryStateChanged event"
  below for why).
- [curl](https://curl.se/) if you want to push messages via the REST Proxy instead of
  `kafka-console-producer` - see "Push a message via curl" below. It ships with Windows 10+
  and macOS/Linux by default.

To start this alongside the Cosmos DB/Service Bus emulators in one step, see
[scripts\setup-podman-local-stack.bat](../setup-podman-local-stack.bat) instead of running the
command below directly.

## Quick start

Run everything (broker + registry + Kafka UI + Kafka REST Proxy + defaults + two test
messages) in one shot:

```bat
scripts\local-kafka\setup-podman-kafka.bat [WarehouseId] [Sku] [Quantity] [EventType] [ConsumerGroup]
```

All five arguments are optional and default to
`WH1 SKU-123 10 Create inventory-events-consumer`. The script also starts **Kafka UI** on
`http://localhost:8090` and **Kafka REST Proxy** on `http://localhost:8086`, and calls
[registration/register-defaults.bat](registration/register-defaults.bat) (see below) to pre-create the app's other
default topics/consumer groups/schema, then publishes **two** test messages: the plain-JSON
one, which it **consumes** under `ConsumerGroup` - so that group shows up in Kafka UI with
a real committed offset against this specific message, not just an empty/never-consumed one
- and a second, Avro-encoded `InventoryStateChanged` event (with `Correlation-Id`/
`Deduplication-Id`/`Type`/`App-Id` headers) produced via Kafka REST Proxy's v3 API and
likewise **consumed** into the `$InventoryStateChanged` group - see
[registration/push-inventory-state-changed.ps1](registration/push-inventory-state-changed.ps1)
and "Producing a real InventoryStateChanged event" below for the manual/curl equivalent
this automates. The script is idempotent (safe to re-run), but not in the sense of reusing
everything: the Podman **network** and the broker's **data volume**
(`iis-wms-kafka-data` - topics/messages/consumer offsets/registered schemas) are both
reused if they already exist - only the broker, Schema Registry, Kafka UI, and Kafka REST
Proxy **containers** are always removed and recreated fresh on every run, so a re-run
always picks up the current listener/credential config instead of silently continuing on a
stale one (a recurring gotcha before this) while the underlying data survives the
container recreation. Schema Registry is still recreated alongside the broker on every run
regardless - its registered schemas live in the broker's own `_schemas` topic, not in the
Schema Registry container itself, so there'd be nothing gained by keeping the Schema
Registry container around separately. If you want a genuinely empty broker (no persisted
topics/messages/offsets/schemas), delete the volume yourself first - see
[Cleanup](#cleanup). To change the local credentials themselves, edit
[config/credentials.json](config/credentials.json) - see
[Configuring credentials](#configuring-credentials) below - the same steps the script runs
are broken out individually further down.

Kafka UI and Kafka REST Proxy here each run as their own container (not sharing a Pod
network namespace the way [kafka-pod.yaml](kafka-pod.yaml)'s does - see that section
below), which needs a dedicated **third Kafka listener**, `SASL_PLAINTEXT_NET` on port 9093
(in-network only, not published to the host) - reusing the host-facing
`SASL_PLAINTEXT_HOST` listener doesn't work here, because that listener advertises
`localhost:9092`, and "localhost" from inside either container isn't the broker. See the
comments in [setup-podman-kafka.bat](setup-podman-kafka.bat)'s Kafka broker section for the
full explanation if you're adapting this elsewhere.

## Configuring ports

Every host-side port `setup-podman-kafka.bat` publishes - the broker, Schema Registry,
Kafka UI, Kafka REST Proxy, and the `events-api.ps1` wrapper - comes from
[../ports.env](../ports.env), a plain `KEY=VALUE` file (one setting per line, `#` for
comments) **shared with**
[../local-emulators/setup-podman-emulators.bat](../local-emulators/setup-podman-emulators.bat)
(see [../local-emulators/README.md](../local-emulators/README.md)'s own "Configuring ports"
section for that script's keys in the same file):

```
KAFKA_BROKER_PORT=9092
SCHEMA_REGISTRY_PORT=8085
KAFKA_UI_PORT=8090
KAFKA_REST_PORT=8086
EVENTS_API_PORT=8087
```

Edit a value there and re-run `setup-podman-kafka.bat` to remap it - useful when a default
already conflicts with something else already running on your machine (another local Kafka
broker, a different container stack, etc). The script falls back to the defaults above for
any key that's missing or if the file itself is absent, so `ports.env` only needs to declare
the values you actually want to change.

Only the **host-side** mapping is configurable - each container's own internal port stays
fixed, so this only ever helps with port conflicts on your machine, not anything
container-to-container (Kafka UI/REST Proxy still reach the broker over the in-network
`SASL_PLAINTEXT_NET` listener on its fixed port 9093, regardless of `KAFKA_BROKER_PORT`).
`KAFKA_BROKER_PORT` is the one exception worth calling out: changing it also updates
`KAFKA_ADVERTISED_LISTENERS`' `SASL_PLAINTEXT_HOST` value to match, since that's the address
Kafka hands back to a client on its first connect for every subsequent reconnect - a stale
value there would silently send clients back to the old host port.

**Changing a port here does not update `appsettings.json`/`appsettings.Development.json` or
your user-secrets** - if you move `Kafka:BootstrapServers`/`Kafka:SchemaRegistryUrl` onto a
non-default port, update those to match yourself, or the app will keep pointing at the old
one. The script's own console output at the end of a run always reflects the ports actually
in effect, so treat that as the source of truth if you're unsure what to put in config.

This only applies to the `.bat` script - the `kube play` ([kafka-pod.yaml](kafka-pod.yaml))
and custom-image setups below still use their own fixed ports; see those sections' own
`podman run`/`kube play` commands if you need to remap a port there instead.

## Configuring credentials

The broker's SASL/PLAIN username/password, Schema Registry's HTTP Basic Auth username/
password, and the Nexus deduplication mock's client id/secret all come from
[config/credentials.json](config/credentials.json):

```json
{
  "Kafka": {
    "Username": "kafkaclient",
    "Password": "kafkaclient-secret",
    "SchemaRegistryApiKey": "schemaregistry",
    "SchemaRegistryApiSecret": "schemaregistry-secret"
  },
  "Nexus": {
    "Deduplication": {
      "ClientId": "iis-wms-consumer",
      "ClientSecret": "iis-wms-consumer-secret"
    }
  }
}
```

Field names match the real `appsettings.json`/`appsettings.Development.json` config paths
1:1 (`Kafka:Username`/`Password`/`SchemaRegistryApiKey`/`SchemaRegistryApiSecret`,
`Nexus:Deduplication:ClientId`/`ClientSecret`) - there's no separate naming scheme to
translate when copying a value into user-secrets. Edit a value and re-run
`setup-podman-kafka.bat` to pick it up (it regenerates the JAAS/properties files under
`secrets\` - see [Generate local credential files](#1-generate-local-credential-files) below
- and recreates the broker/Schema Registry containers on every run regardless, so a
credential change always takes effect). The script falls back to the defaults shown above
for any key that's missing or if the file itself is absent, so `credentials.json` only needs
to declare the values you actually want to change.

**`Nexus:Deduplication:ClientId`/`ClientSecret` aren't enforced by anything this script
starts** - `events-api.ps1`'s Nexus mock deliberately accepts any `client_id`/`client_secret`
on `POST /oauth/token` (it's a local testing double, not a security boundary - see
"Nexus deduplication mock" below). They're recorded here purely so this file is the one
place documenting the value `appsettings.Development.json`/user-secrets should actually
carry for `Nexus:Deduplication:ClientId`/`ClientSecret` to match what a real Nexus
environment would issue.

Like [ports.env](../ports.env), this file is local-only, throwaway credentials - never the
real Confluent Cloud/Nexus ones from `appsettings.json` - so it's safe to commit and safe to
edit freely. This only applies to the `.bat` script; the `kube play`/custom-image setups
below bake their own copies of these same default values in inline instead (see their own
sections) and would need editing separately if you want different credentials there too.

### Push a message via curl (Kafka REST Proxy)

`kafka-console-producer` (the "Push a message" step below) speaks Kafka's own wire
protocol, not HTTP - there's no way to `curl` a message directly into the broker. Kafka
REST Proxy ([confluentinc/cp-kafka-rest](https://docs.confluent.io/platform/current/kafka-rest/index.html))
bridges that gap: it's an HTTP service the `.bat` and `kube play` setups both start, which
itself authenticates to the broker over SASL (using the same `kafkaclient` credentials, via
`KAFKA_REST_CLIENT_*` settings - see `secrets\kafka-rest.env`), so you can `curl` a message
in instead.

The REST Proxy's own HTTP API has **no Basic Auth layer configured here** - same reasoning
as Kafka UI's `http://localhost:8090` (see the settings table above): it's a local-only
convenience endpoint, open to anyone who can reach that port, that internally holds the
real broker credentials.

`inventory-events` (the default topic) carries plain JSON, not Avro (see
[InboundInventoryEventMessage](../../src/Infrastructure/IIS.WMS.Consumer.Infrastructure/Messaging/InboundInventoryEventMessage.cs)),
so the REST Proxy's JSON embedded format is the right fit - no Schema Registry round-trip
needed for this topic. Write the body to a file first rather than inlining it as a `curl`
`-d` argument, for the same `cmd.exe` quoting reasons as the console-producer step below:

```bat
echo {"records":[{"value":{"EventId":"evt-curl-001","WarehouseId":"WH1","Sku":"SKU-123","Quantity":10,"EventType":"Create"}}]}> "%TEMP%\message-rest.json"

curl -X POST http://localhost:8086/topics/inventory-events -H "Content-Type: application/vnd.kafka.json.v2+json" --data-binary "@%TEMP%\message-rest.json"
```

`--data-binary "@file"` vs. inlining the JSON directly is about how curl *reads* the
payload (a file, byte-for-byte, vs. a command-line string), not about "JSON vs. binary" -
the wire format is controlled entirely by the `Content-Type` header either way; this is
already posting as JSON. The file is only there to dodge `cmd.exe`'s quoting problem with
the JSON's embedded double quotes (same reasoning as the JAAS config files elsewhere in
this folder). If you're running this from **PowerShell** instead, its quoting rules don't
have that problem, so you can skip the file and inline the JSON directly, in a single
quoted string:

```powershell
curl.exe -X POST http://localhost:8086/topics/inventory-events -H "Content-Type: application/vnd.kafka.json.v2+json" --data-raw '{"records":[{"value":{"EventId":"evt-curl-001","WarehouseId":"WH1","Sku":"SKU-123","Quantity":10,"EventType":"Create"}}]}'
```

(`curl.exe`, not the `curl` alias PowerShell maps to `Invoke-WebRequest` by default in some
setups - if plain `curl` doesn't behave like the command above, use `curl.exe` explicitly.)

A successful response looks like:

```json
{"offsets":[{"partition":0,"offset":1,"error_code":null,"error":null}],"key_schema_id":null,"value_schema_id":null}
```

Other useful REST Proxy calls:

```bat
REM List topics
curl http://localhost:8086/topics

REM List partitions for a topic
curl http://localhost:8086/topics/inventory-events/partitions
```

There's no REST endpoint to *consume* into `appsettings.json`'s actual consumer groups
here (Kafka REST Proxy's own consumer API creates its own ad-hoc REST-only consumer
instances) - use Kafka UI's Messages tab, or the `kafka-console-consumer` steps elsewhere
in this file, to verify a message that was produced via curl.

#### Sending Kafka headers via curl (v3 Produce API)

The `/topics/{topic}` endpoint above (the REST Proxy's "v2" Produce API) only accepts HTTP
headers (`Content-Type`) - it has no way to set actual **Kafka record headers**, so a
message produced that way always arrives with none of `Correlation-Id`/
`Deduplication-Id`/`Type` that
[KafkaConsumerHostedServiceBase.cs](../../src/Infrastructure/IIS.WMS.Consumer.Infrastructure/Messaging/Kafka/KafkaConsumerHostedServiceBase.cs)
optionally reads (see the "No Kafka headers" note in step 7 below) - it always falls back
to a fresh correlation id, skipped dedup, and the default schema handler. To set those from
curl, use the REST Proxy's newer **v3 Produce API** instead, which is header-aware.

**Two common mistakes produce a `400` here** (Confluent's error names the unrecognized
field, e.g. `"Unrecognized field \"channel\" ... 6 known properties: \"value\",
\"originalSize\", \"partitionId\", \"headers\", \"key\", \"timestamp\""`):

1. Passing `correlationId`/`dedupId`/`type`/`appId` as **HTTP request headers**
   (`-H`/`--header`/Postman's Headers tab). Those just become HTTP headers on the call to
   the REST Proxy's own endpoint - the REST Proxy never reads them and never copies them
   onto the Kafka record. The only place to set actual Kafka record headers is the request
   **body's** `headers` array (below) - and the names must match
   [WellKnownHeaderNames.cs](../../src/Common/IIS.WMS.Common/Messaging/WellKnownHeaderNames.cs)
   exactly: `Correlation-Id`, `Deduplication-Id`, `Type`, `App-Id` - not `correlationId`/
   `dedupId`/`type`/`appId`.
2. Posting the event payload as the **top-level** request body (e.g. `{"channel": ...,
   "id": ..., ...}` directly). The v3 API's top-level body is the `ProduceRequest`
   envelope, not the event - the actual payload has to be nested one level down, at
   `value.data`.

First, find the cluster id (fixed per broker - look it up once and reuse it):

```bat
curl http://localhost:8086/v3/clusters
```

Kafka header *values* must be base64-encoded bytes in this API, so build the request body
with PowerShell rather than hand-encoding it:

```powershell
$headers = @(
    @{ name = 'Correlation-Id'; value = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes('corr-curl-001')) },
    @{ name = 'Deduplication-Id'; value = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes('evt-curl-002')) }
)
$body = @{
    value   = @{ type = 'JSON'; data = @{ EventId = 'evt-curl-002'; WarehouseId = 'WH1'; Sku = 'SKU-123'; Quantity = 10; EventType = 'Create' } }
    headers = $headers
} | ConvertTo-Json -Depth 5 -Compress
```

Then POST it, substituting `<cluster_id>` from the `v3/clusters` call above. Since `$body`
is already a PowerShell string, pass it to curl directly - no file needed here, unlike the
`cmd.exe` example above. curl's `--json` flag (curl 7.82+; check with `curl.exe --version`
if unsure) is a shorthand for `--data-binary` plus `Content-Type`/`Accept:
application/json` - and this API's Content-Type genuinely is plain `application/json`
(unlike the v2 endpoint above), so it's a clean fit here:

```powershell
curl.exe -X POST "http://localhost:8086/v3/clusters/<cluster_id>/topics/inventory-events/records" --json $body
```

Or, on an older curl without `--json`, the equivalent explicit form:

```powershell
curl.exe -X POST "http://localhost:8086/v3/clusters/<cluster_id>/topics/inventory-events/records" -H "Content-Type: application/json" --data-raw $body
```

To stay in `cmd.exe` throughout instead, write `$body` to a file first:

```powershell
$body | Out-File -Encoding utf8 "$env:TEMP\message-v3.json"
```

```bat
curl -X POST "http://localhost:8086/v3/clusters/<cluster_id>/topics/inventory-events/records" -H "Content-Type: application/json" --data-binary "@%TEMP%\message-v3.json"
```

#### Producing a real `InventoryStateChanged` event, with all four headers

`setup-podman-kafka.bat` now does this step automatically (see
[registration/push-inventory-state-changed.ps1](registration/push-inventory-state-changed.ps1)) -
this walkthrough is the manual/curl equivalent, useful for producing more than the one
event the script sends, or adapting the shape/headers.

The script itself isn't hardcoded to this one schema either - `-Body` takes a raw JSON
string for the event payload (spliced into the request as-is, not reparsed/reserialized,
so any valid JSON works, pretty-printed or not) and `-SchemaName` takes the Schema
Registry subject to resolve `schema_id` from (previously always `"$Topic-value"`), so the
same script can produce a different schema's event onto a different topic:

```powershell
registration\push-inventory-state-changed.ps1 -Topic other-topic -SchemaName other-topic-value -Body '{"field1":"value1","field2":42}' -EventType 'inventory.OtherEvent'
```

Both default to the `InventoryStateChanged` sample (`inventory-events` /
`inventory-events-value`) if omitted, which is what `setup-podman-kafka.bat` relies on.

##### Simpler still: the `events-api.ps1` wrapper

Everything below this point (`<cluster_id>` lookup, `schema_id` lookup, base64-encoding
headers by hand) is exactly what makes calling Kafka REST Proxy's v3 API directly from
Postman/curl tedious. [registration/events-api.ps1](registration/events-api.ps1) is a small
local HTTP wrapper that does all of it for you, so Postman/curl only ever has to deal with
plain HTTP headers and a raw JSON body:

```
POST /api/events?type=<EventName>
```

Run it once and leave it running while you test:

```bat
powershell -NoProfile -File scripts\local-kafka\registration\events-api.ps1
```

It always runs directly on your host, never inside a container/Pod, so this works
identically no matter which local setup started the underlying Kafka stack - `.bat` or
[kafka-pod.yaml](kafka-pod.yaml)'s `podman kube play` - since both publish REST Proxy on
`localhost:8086` and Schema Registry on `localhost:8085` the same way; the wrapper only
needs those two reachable. It isn't part of either setup itself (not a container in
`kafka-pod.yaml`, not started by `setup-podman-kafka.bat`) and it isn't meant for the real
[k8s/kafka-consumer/](../../k8s/kafka-consumer/) AKS deployment either - it talks to a
local, unauthenticated-to-callers REST Proxy using this folder's throwaway credentials,
which has no equivalent against a real cluster.

**Postman**: import [events-api.postman_collection.json](events-api.postman_collection.json)
(`Import` → select the file) rather than building the request by hand - it already has
the four headers, the body below, and a Pre-request Script that fills in a fresh
`changeDate`/`Correlation-Id`/`Deduplication-Id` on every send (Postman's built-in
`{{$timestamp}}` is seconds, not the milliseconds `changeDate` needs, so that field
specifically has to be computed rather than using a dynamic variable directly).

Building the request by hand instead - `POST
http://localhost:8087/api/events?type=inventory-events-value`,
`Correlation-Id`/`Deduplication-Id`/`Type`/`App-Id` set as **plain HTTP headers** (no
base64 encoding needed - the wrapper does that), and the raw event JSON as the body:

```json
{
  "channel": "OTHER_STORES",
  "id": "294650C0135764824",
  "changeDate": 1752302832319,
  "location": { "id": "TDC", "type": "WAREHOUSE" },
  "entity": null,
  "type": "BLC",
  "fromState": { "state": "AVAILABLE", "status": "PICKABLE" },
  "toState": { "state": "AVAILABLE", "status": "HELD" },
  "itemLines": [
    { "lineNum": "1", "productId": "294650C01", "quantity": 63, "countryOfOrigin": "TH", "hallmarking": "NON" }
  ],
  "referenceId": null
}
```

**curl** - same idea, no base64 encoding of headers needed here either (that's the whole
point of the wrapper), so plain `-H` flags work directly:

```bat
echo {"channel":"OTHER_STORES","id":"294650C0135764824","changeDate":1752302832319,"location":{"id":"TDC","type":"WAREHOUSE"},"entity":null,"type":"BLC","fromState":{"state":"AVAILABLE","status":"PICKABLE"},"toState":{"state":"AVAILABLE","status":"HELD"},"itemLines":[{"lineNum":"1","productId":"294650C01","quantity":63,"countryOfOrigin":"TH","hallmarking":"NON"}],"referenceId":null}> "%TEMP%\message-events-api.json"

curl -X POST "http://localhost:8087/api/events?type=inventory-events-value" -H "Correlation-Id: corr-curl-001" -H "Deduplication-Id: dedup-curl-001" -H "Type: inventory.InventoryStateChanged" -H "App-Id: curl" --data-binary "@%TEMP%\message-events-api.json"
```

(`changeDate` in that example is a fixed, stale value - fine to replay once, but use a
current epoch-millis value for repeat testing, same caveat as everywhere else `changeDate`
shows up in this file.)

##### Nexus deduplication mock (`/oauth/token`, `/nexus/deduper/api/dedupe`)

`events-api.ps1` also mocks the external Nexus deduplication API this consumer calls (see
[NexusDeduplicationService.cs](../../src/Infrastructure/IIS.WMS.Consumer.Infrastructure/NexusServices/NexusDeduplicationService.cs)/
[NexusAuthenticationHandler.cs](../../src/Infrastructure/IIS.WMS.Consumer.Infrastructure/NexusServices/NexusAuthenticationHandler.cs)/
[NexusServiceCollectionExtensions.cs](../../src/Infrastructure/IIS.WMS.Consumer.Infrastructure/NexusServices/NexusServiceCollectionExtensions.cs)),
matching `appsettings.Development.json`'s `Nexus:Deduplication` config, which points both
`BaseUrl` and `OAuthEndpoint` at this same `http://localhost:8087`:

```
POST /oauth/token                   - client-credentials token endpoint
POST /nexus/deduper/api/dedupe      - dedupe check/store endpoint
```

`POST /oauth/token` accepts the form-urlencoded body `NexusAuthenticationHandler` sends
(`grant_type`/`client_id`/`client_secret`/`scope`) without validating the credentials - it's a
local testing double, not a security boundary - and responds with a fresh bearer token:

```json
{ "access_token": "nexus-local-...", "expires_in": 3600 }
```

`POST /nexus/deduper/api/dedupe` requires that token in an `Authorization: Bearer
<access_token>` header (`401` if it's missing, malformed, or unknown/expired), and a body of
`{ "dedupeId": "<value>" }` (matches `NexusDeduplicationService`'s request shape). The first
request for a given `dedupeId` is stored in memory and answered `201 Created`; a repeat of the
same `dedupeId` is answered `409 Conflict` - the only two outcomes `NexusDeduplicationService`
distinguishes (only `409` means "duplicate"; any other non-success status is treated as a Nexus
outage). Both the issued-token set and the dedupe cache live only in this process's memory, so
restarting it forgets everything.

```bat
curl -X POST http://localhost:8087/oauth/token -d "grant_type=client_credentials&client_id=iis-wms-consumer&client_secret=iis-wms-consumer-secret&scope=dedup.readwrite"

curl -X POST http://localhost:8087/nexus/deduper/api/dedupe -H "Authorization: Bearer <access_token from above>" -H "Content-Type: application/json" -d "{\"dedupeId\":\"test-123\"}"
```

The wrapper resolves the topic from `type` by stripping a trailing `-value` (Confluent's
default subject-naming convention - `inventory-events-value` → topic `inventory-events`);
it forwards every request header you send (other than the usual HTTP/framework ones -
`Content-Type`, `Host`, `Accept`, etc.) as a Kafka record header, base64-encoding each one
automatically. It's not started by `setup-podman-kafka.bat` - run it yourself when you
want it, `Ctrl+C` to stop, and it needs Kafka REST Proxy/Schema Registry already running
(the rest of the setup). It also isn't schema-specific: point it at any registered
subject's name via `type` to produce a different event/topic, same as
`push-inventory-state-changed.ps1 -SchemaName`/`-Body` above - it's the same idea as an
always-on HTTP endpoint instead of a one-shot script.

Putting the two fixes above together - all four headers
[WellKnownHeaderNames.cs](../../src/Common/IIS.WMS.Common/Messaging/WellKnownHeaderNames.cs)
reads, moved into the `headers` array, and the event payload nested under `value.data`:

```powershell
function New-Base64Header([string]$name, [string]$value) {
    @{ name = $name; value = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($value)) }
}

$body = @{
    value = @{
        type = 'JSON'
        data = @{
            channel    = 'OTHER_STORES'
            id         = '294650C0135764824'
            changeDate = '2026-07-11T08:48:06.000Z'
            location   = @{ id = 'TDC'; type = 'WAREHOUSE' }
            entity     = $null
            type       = 'BLC'
            fromState  = @{ state = 'AVAILABLE'; status = 'PICKABLE' }
            toState    = @{ state = 'AVAILABLE'; status = 'HELD' }
            itemLines  = @(
                @{
                    lineNum = '1'; productId = '294650C01'; quantity = 63
                    countryOfOrigin = 'TH'; hallmarking = 'NON'
                }
            )
            referenceId = $null
        }
    }
    headers = @(
        New-Base64Header 'Correlation-Id' '57f3f043-e9bc-41c0-be8f-3a43d56e95ce'
        New-Base64Header 'Deduplication-Id' 'dedup-421659c1-3ffe-4f15-86f2-84786c21e4d1'
        New-Base64Header 'Type' 'inventory.InventoryStateChanged'
        New-Base64Header 'App-Id' 'TestApp'
    )
} | ConvertTo-Json -Depth 6 -Compress

curl.exe -X POST "http://localhost:8086/v3/clusters/<cluster_id>/topics/inventory-events/records" --json $body
```

This will get past the `400` and produce successfully - but with `value.type: 'JSON'` it
won't actually be *consumed* correctly: the `$InventoryStateChanged` consumer group's
[InventoryStateChangedConsumerHostedService](../../src/Infrastructure/IIS.WMS.Consumer.Infrastructure/Messaging/Kafka/InventoryStateChangedConsumerHostedService.cs)
expects a genuine **Avro**-encoded value (Confluent wire format - magic byte + schema id +
Avro binary, via Schema Registry), not embedded JSON - it doesn't inspect the `Type`
header to pick a schema at all (it registers its one handler under `DefaultEventType`, so
it processes *any* message on this topic as Avro regardless of that header's value - see
the class's own doc comment). A message produced with `value.type: 'JSON'` will get a
`200` from the REST Proxy, then fail Avro deserialization downstream and land in the
hot-tier dead-letter blob container instead of reaching Service Bus.

To actually produce as Avro instead, get the registered schema id for subject
`inventory-events-value` and reference it as `value.schema_id` - **without** an explicit
`value.type` alongside it: pairing an explicit `type` with any schema reference
(`schema_id` here, `schema_version` in
[this upstream bug](https://github.com/confluentinc/kafka-rest/issues/1028)) 400s with
`"'schema_id=N' cannot be used with 'serializer'"` - `schema_id` alone already uniquely
identifies this as Avro, and the response echoes `"type":"AVRO"` back regardless:

```powershell
$schemaId = (Invoke-RestMethod -Uri 'http://localhost:8085/subjects/inventory-events-value/versions/latest' -Headers @{ Authorization = 'Basic ' + [Convert]::ToBase64String([Text.Encoding]::ASCII.GetBytes('schemaregistry:schemaregistry-secret')) }).id

$body = @{
    value = @{
        schema_id = $schemaId
        # changeDate must be epoch milliseconds (a JSON number), not an ISO-8601 string -
        # the .avsc declares it as {"type": "long", "logicalType": "timestamp-millis"} and
        # the REST Proxy 400s with "Expected long. Got VALUE_STRING" otherwise.
        data      = @{ channel = 'OTHER_STORES'; id = '294650C0135764824'; changeDate = [DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds(); location = @{ id = 'TDC'; type = 'WAREHOUSE' }; entity = $null; type = 'BLC'; fromState = @{ state = 'AVAILABLE'; status = 'PICKABLE' }; toState = @{ state = 'AVAILABLE'; status = 'HELD' }; itemLines = @(@{ lineNum = '1'; productId = '294650C01'; quantity = 63; countryOfOrigin = 'TH'; hallmarking = 'NON' }); referenceId = $null }
    }
    headers = @(
        New-Base64Header 'Correlation-Id' '57f3f043-e9bc-41c0-be8f-3a43d56e95ce'
        New-Base64Header 'Deduplication-Id' 'dedup-421659c1-3ffe-4f15-86f2-84786c21e4d1'
        New-Base64Header 'Type' 'inventory.InventoryStateChanged'
        New-Base64Header 'App-Id' 'TestApp'
    )
} | ConvertTo-Json -Depth 6 -Compress

curl.exe -X POST "http://localhost:8086/v3/clusters/<cluster_id>/topics/inventory-events/records" --json $body
```

This needs the REST Proxy itself to authenticate to Schema Registry (a separate concern
from it authenticating to the *broker* via SASL) -
`KAFKA_REST_SCHEMA_REGISTRY_BASIC_AUTH_CREDENTIALS_SOURCE`/`_USER_INFO` in
`kafka-rest.env`/`kafka-pod.yaml` cover that. `setup-podman-kafka.bat` now removes and
recreates `iis-wms-kafka-rest` on every run specifically so it always picks up the current
`kafka-rest.env` - env vars only take effect at container creation, and this container
(unlike the broker/registry/UI) has no state worth preserving across runs, so there's no
downside to always starting it fresh. Doing this manually (e.g. against `kafka-pod.yaml`
or after editing `kafka-rest.env` by hand): `podman rm -f iis-wms-kafka-rest` and re-run.

> The request/response field names above (`schema_id`, `partition_id`, `error_code`, etc. -
> all **snake_case**) are confirmed against confluentinc/kafka-rest's own
> [api/v3/openapi.yaml](https://github.com/confluentinc/kafka-rest/blob/master/api/v3/openapi.yaml)
> and real captured example responses in that repo, not guessed - an earlier version of
> this doc/script used camelCase (`schemaId`/`partitionId`) based on a misreading of a
> Jackson "unrecognized field" error (which names fields via the Java class's introspected
> property name, not necessarily the wire JSON name) and that broke it. What's still
> confirmed against a live Kafka REST Proxy 7.6.0 instance: pairing an explicit
> `value.type` with a schema reference (`schema_id` here) 400s with `"'schema_id=N' cannot
> be used with 'serializer'"` - the exact same bug class as
> [this upstream issue](https://github.com/confluentinc/kafka-rest/issues/1028), just
> triggered by `schema_id` instead of `schema_version: "latest"`. Omitting `type` (as the
> examples above now do) is the confirmed-working form. The Produce API also returns HTTP
> `200` even when an individual record is rejected - the real outcome is only in the
> response body's `error_code`/`message`, which is why
> [push-inventory-state-changed.ps1](registration/push-inventory-state-changed.ps1) checks
> that field explicitly rather than trusting the HTTP status.

> **Confirmed bug in Kafka REST Proxy 7.6.0's Avro handling**, found via
> `podman logs iis-wms-kafka-rest` after an `error_code: 50002` ("Error serializing Avro
> message") response: for a field with an Avro `logicalType` (here, `changeDate`'s
> `timestamp-millis`), 7.6.0's v3 JSON→Avro conversion internally turns the value into a
> `java.time.Instant` and then crashes trying to write it back as the underlying `long` -
> `ClassCastException: value ... (a java.time.Instant) cannot be cast to expected type
> long at InventoryStateChanged.changeDate`. This happens regardless of whether the JSON
> value is a number or a string - there's no request-shape workaround. That's why
> `KAFKA_REST_IMAGE`/`kafka-pod.yaml`'s `kafka-rest` container are pinned to
> `confluentinc/cp-kafka-rest:8.2.2`, deliberately newer than the broker/Schema Registry's
> `7.6.0` (Confluent Platform supports running REST Proxy at a different version than the
> rest of the cluster) - **confirmed fixed on 8.2.2**: `push-inventory-state-changed.ps1`
> produces this event successfully against it. If a future image bump reintroduces the
> same `ClassCastException` in `podman logs iis-wms-kafka-rest`, the next thing to try is
> `kafka-avro-console-producer` (bundled in the
> `cp-schema-registry` image) instead of the REST Proxy for this specific message.

#### Using Postman instead of curl

Every REST Proxy call above is plain HTTP, so Postman works just as well as `curl` - and
for the v3 endpoints, arguably better, since editing a real JSON body in Postman's editor
beats hand-escaping one for `cmd.exe`. Postman can also **import a `curl` command
directly** (`Import` → paste as raw text) if you'd rather start from one of the commands
above than build a request by hand.

**Building requests manually:**

| Call | Method | URL | Headers | Auth |
|---|---|---|---|---|
| Push plain JSON (v2) | POST | `http://localhost:8086/topics/inventory-events` | `Content-Type: application/vnd.kafka.json.v2+json` | none |
| List clusters | GET | `http://localhost:8086/v3/clusters` | - | none |
| Get schema id | GET | `http://localhost:8085/subjects/inventory-events-value/versions/latest` | - | Basic: `schemaregistry` / `schemaregistry-secret` (use Postman's **Authorization tab → Basic Auth**, not a hand-built header) |
| Push `InventoryStateChanged` (v3, Avro + headers) | POST | `http://localhost:8086/v3/clusters/<cluster_id>/topics/inventory-events/records` | `Content-Type: application/json` | none |

For the v2 call, the **Body** tab (raw, JSON):

```json
{
  "records": [
    { "value": { "EventId": "evt-postman-001", "WarehouseId": "WH1", "Sku": "SKU-123", "Quantity": 10, "EventType": "Create" } }
  ]
}
```

For the v3 `InventoryStateChanged` call, `<cluster_id>` and the `schema_id` come from the
two GET calls above (copy `data[0].cluster_id` and `id` out of their responses). Kafka
header values must be base64 - these ones decode to `Correlation-Id: 57f3f043-e9bc-41c0-
be8f-3a43d56e95ce`, `Deduplication-Id: dedup-421659c1-3ffe-4f15-86f2-84786c21e4d1`,
`Type: inventory.InventoryStateChanged`, `App-Id: TestApp` (same values used elsewhere in
this section), precomputed so there's nothing to encode by hand:

```json
{
  "value": {
    "schema_id": 1,
    "data": {
      "channel": "OTHER_STORES",
      "id": "294650C0135764824",
      "changeDate": 1752302832319,
      "location": { "id": "TDC", "type": "WAREHOUSE" },
      "entity": null,
      "type": "BLC",
      "fromState": { "state": "AVAILABLE", "status": "PICKABLE" },
      "toState": { "state": "AVAILABLE", "status": "HELD" },
      "itemLines": [
        { "lineNum": "1", "productId": "294650C01", "quantity": 63, "countryOfOrigin": "TH", "hallmarking": "NON" }
      ],
      "referenceId": null
    }
  },
  "headers": [
    { "name": "Correlation-Id", "value": "NTdmM2YwNDMtZTliYy00MWMwLWJlOGYtM2E0M2Q1NmU5NWNl" },
    { "name": "Deduplication-Id", "value": "ZGVkdXAtNDIxNjU5YzEtM2ZmZS00ZjE1LTg2ZjItODQ3ODZjMjFlNGQx" },
    { "name": "Type", "value": "aW52ZW50b3J5LkludmVudG9yeVN0YXRlQ2hhbmdlZA==" },
    { "name": "App-Id", "value": "VGVzdEFwcA==" }
  ]
}
```

Replace `"schema_id": 1` with whatever `id` Schema Registry actually returned - `1` is
only correct against a freshly-registered local stack. `changeDate` needs to be a current
epoch-millis value, not this stale example - Postman's `{{$timestamp}}` dynamic variable
(seconds, not millis) won't match `long`/`timestamp-millis`'s precision, so compute it
yourself (e.g. `Date.now()` in a browser console) rather than relying on a built-in.

**Nicer: resolve `cluster_id`/`schema_id`/headers automatically with a Pre-request
Script.** Rather than copy-pasting from two GET calls and pre-encoding headers by hand
every time, add this to the v3 request's **Pre-request Script** tab - it populates
environment variables the URL/body can then reference as `{{cluster_id}}`/`{{schema_id}}`/
`{{correlationIdB64}}`/etc.:

```javascript
pm.sendRequest('http://localhost:8086/v3/clusters', (err, res) => {
    pm.environment.set('cluster_id', res.json().data[0].cluster_id);
});

pm.sendRequest({
    url: 'http://localhost:8085/subjects/inventory-events-value/versions/latest',
    method: 'GET',
    header: { Authorization: 'Basic ' + btoa('schemaregistry:schemaregistry-secret') }
}, (err, res) => {
    pm.environment.set('schema_id', res.json().id);
});

pm.environment.set('correlationIdB64', btoa(pm.variables.replaceIn('{{$guid}}')));
pm.environment.set('deduplicationIdB64', btoa('dedup-' + pm.variables.replaceIn('{{$guid}}')));
pm.environment.set('typeB64', btoa('inventory.InventoryStateChanged'));
pm.environment.set('appIdB64', btoa('Postman'));
```

Then set the request URL to
`http://localhost:8086/v3/clusters/{{cluster_id}}/topics/inventory-events/records` and
swap the body's `"schema_id": 1` for `"schema_id": {{schema_id}}` (unquoted - Postman's
variable substitution is plain text, so this resolves to a JSON number, not a string) and
each header `value` for its `{{...B64}}` counterpart. `pm.sendRequest` is asynchronous,
but Postman waits for the pre-request script to finish before firing the main request, so
this is safe to rely on.

### Registering defaults on their own

[registration/register-defaults.bat](registration/register-defaults.bat) creates the app's default topics
(`inventory-events`, `inventory-bulk-import`), pre-creates its default consumer groups
(`inventory-events-consumer`, `$InventoryStateChanged`, `inventory-bulk-import-consumer` -
matching `appsettings.json`'s actual `Kafka:ConsumerGroup` values) with a committed offset
so they appear in Kafka UI's Consumers tab immediately, and registers a schema for subject
`inventory-events-value` from [registration/inventory-state-changed.avsc](registration/inventory-state-changed.avsc)
- a **simplified, local-test version** of the real Avro contract
`InventoryStateChangedConsumerHostedService.cs` deserializes (the actual one ships inside
the `NexusFacades.Common.AvroSchemas` NuGet package - this file keeps the same field
names/nesting but drops the production doc comments and the largest enums, `CountryCode`/
`CurrencyCode`, to stay maintainable). `setup-podman-kafka.bat` calls it automatically; run
it directly against an already-running stack (e.g. the `kube play` or custom-image setups
below) with:

```bat
scripts\local-kafka\registration\register-defaults.bat [KafkaContainer] [SchemaRegistryHostPort]
```

Defaults: `iis-wms-kafka 8085` (the `.bat`/image path's container name). For the `kube
play` setup, pass the Pod's actual container name instead - see "Find the container name"
below, e.g. `registration\register-defaults.bat iis-wms-local-kafka-kafka`.

> **Note:** I haven't been able to run this SASL/Basic-Auth setup against a live Podman
> Desktop install myself (no `podman` binary in the environment I authored it in) - if a
> step below fails, check the Troubleshooting section, and let me know what error you hit.

## Setup via Kubernetes YAML (`podman kube play`)

[kafka-pod.yaml](kafka-pod.yaml) is a declarative alternative to the `.bat` script's
`podman run` calls - same broker/registry, same credentials, same ports, but expressed as
a Kubernetes `Pod` + `Secret` that Podman itself knows how to apply and tear down. Unlike
the `.bat` script, it's fully self-contained - the credential files are inline in the
`Secret`, not generated on disk first, and Kafka + Schema Registry run as containers in
**one** Pod rather than separate containers on a custom network, since Pod containers
already share one network namespace (so Schema Registry just reaches the broker at
`localhost:29092`, no extra network object needed).

Like the `.bat` script, this also includes a **Kafka UI** container -
[kafbat/kafka-ui](https://github.com/kafbat/kafka-ui) (the actively maintained
continuation of `provectuslabs/kafka-ui`, which stopped receiving updates) - and a **Kafka
REST Proxy** container, both pre-wired with the same broker and Schema Registry
credentials, but simpler here than in the `.bat` script, since both share this Pod's
network namespace with the broker and registry: they connect over plain
`localhost:9092`/`localhost:8081`, no separate in-network listener needed the way the
`.bat` script's `SASL_PLAINTEXT_NET` listener is (see Quick start above). Once the Pod is
up, browse to **http://localhost:8090** (Kafka UI) to view topics, browse messages, and
check consumer group lag, or `curl` **http://localhost:8086** (REST Proxy - see "Push a
message via curl" above, same commands, same port) to push a message - neither has a
separate login configured, both are open to anyone who can reach that port. Run
[registration/register-defaults.bat](registration/register-defaults.bat) (see "Registering defaults on their own"
above) against this Pod's container to see the app's default topics/consumer
groups/schema there right away, instead of empty until something actually produces or
consumes.

### Apply it

```bat
podman kube play scripts\local-kafka\kafka-pod.yaml
```

This is idempotent the same way `podman run` is not: re-running `kube play` against a Pod
that's still running updates it in place rather than erroring.

### Find the container name

`podman kube play` names each container `<pod-name>-<container-name>`, but confirm before
relying on it - it can vary by Podman version:

```bat
podman ps --filter pod=iis-wms-local-kafka
```

The rest of this section assumes `iis-wms-local-kafka-kafka` for the broker container -
substitute whatever `podman ps` actually shows.

### Register defaults, push a message

```bat
scripts\local-kafka\registration\register-defaults.bat iis-wms-local-kafka-kafka
```

Unlike `setup-podman-kafka.bat`, this doesn't wait for the broker first - give the Pod a
few seconds after `kube play` before running it, or just re-run it if it fails with a
connection error.

Then push a test message and consume it into a group, same commands as the manual steps
below, just against the Pod's container instead of `iis-wms-kafka`:

```bat
echo {"EventId":"evt-001","WarehouseId":"WH1","Sku":"SKU-123","Quantity":10,"EventType":"Create"}> "%TEMP%\message.json"
podman exec -i iis-wms-local-kafka-kafka kafka-console-producer --bootstrap-server localhost:9092 --producer.config /etc/kafka/secrets/client.properties --topic inventory-events < "%TEMP%\message.json"

podman exec iis-wms-local-kafka-kafka kafka-console-consumer --bootstrap-server localhost:9092 --consumer.config /etc/kafka/secrets/client.properties --topic inventory-events --group inventory-events-consumer --max-messages 1 --timeout-ms 10000
```

### Tear down

```bat
podman kube down scripts\local-kafka\kafka-pod.yaml
podman pod ls
podman pod rm -f iis-wms-local-kafka
```

`kube down` removes exactly what that file's `kube play` created (Pod + Secret) - no
separate network/container cleanup needed, unlike the `.bat` script's manual `podman rm`/
`podman network rm`. It does **not** remove the `iis-wms-kafka-data` named volume the
kafka container's `persistentVolumeClaim` maps onto - that's the point, so a later
`kube play` picks up right where you left off. Delete it explicitly
(`podman volume rm iis-wms-kafka-data`) for a genuinely empty broker.

## Setup via custom images (`podman build`)

[image/kafka.Containerfile](image/kafka.Containerfile) and
[image/schema-registry.Containerfile](image/schema-registry.Containerfile) bake the same
BootstrapServers/SASL and SchemaRegistryUrl/Basic-Auth configuration as the `.bat` script
into two custom images, via `COPY` (the four credential files, same content as the
"Generate local credential files" step below) and `ENV` (the listener/JAAS/auth settings)
- so `podman run` no longer needs a long `-e` flag list at all. Same local-only
credentials as everywhere else in this folder (`kafkaclient`/`kafkaclient-secret`,
`schemaregistry`/`schemaregistry-secret`); any `ENV` can still be overridden with
`podman run -e KEY=value` for a one-off different value.

**Neither Kafka UI nor Kafka REST Proxy is included in this path** - `kafka.Containerfile`
only bakes in the two-listener setup (`SASL_PLAINTEXT_HOST` + the unauthenticated
in-network `PLAINTEXT`), not the third `SASL_PLAINTEXT_NET` listener the `.bat` script adds
for them. Use the `.bat` or `kube play` setup if you want either, or add your own
`podman run` for `docker.io/kafbat/kafka-ui:v1.5.0` / `confluentinc/cp-kafka-rest:8.2.2`
following the `.bat` script's pattern (steps 4a/4b below show both).

### Build

```bat
podman build -t iis-wms-kafka-local -f scripts\local-kafka\image\kafka.Containerfile scripts\local-kafka\image
podman build -t iis-wms-schema-registry-local -f scripts\local-kafka\image\schema-registry.Containerfile scripts\local-kafka\image
```

### Run

```bat
podman network create iis-wms-kafka-net
podman run -d --name iis-wms-kafka --network iis-wms-kafka-net -p 9092:9092 iis-wms-kafka-local
podman run -d --name iis-wms-schema-registry --network iis-wms-kafka-net -p 8085:8081 iis-wms-schema-registry-local
```

The container names (`iis-wms-kafka`, `iis-wms-schema-registry`) matter - both
Containerfiles bake in `KAFKA_ADVERTISED_LISTENERS`/`SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS`
values that assume those exact names on a shared network. Rename them and you'll need to
override the corresponding `ENV` with `-e` at `podman run` to match.

From here, the "Wait for the broker", "Create the topic", "Push a message", and "Verify"
steps below are identical - same container names, same `--command-config`/
`--producer.config` paths (`/etc/kafka/secrets/client.properties` is baked into the image
at the same path the mounted-volume approach used). Since the container is named
`iis-wms-kafka`, the same as the `.bat` script's default, `register-defaults.bat` (no
arguments needed) works as-is against this setup too.

### Tear down

```bat
podman rm -f iis-wms-kafka iis-wms-schema-registry
podman network rm iis-wms-kafka-net
podman rmi iis-wms-kafka-local iis-wms-schema-registry-local
```

## Step by step (manual, via individual `podman run` commands)

### 1. Generate local credential files

The broker and registry both read their credentials from files mounted into the
container, rather than an inline command-line value, to avoid the quoting problems a
JAAS config string (embedded quotes/spaces/semicolons) causes in `cmd.exe`. Create a
`secrets` folder next to the script and write four files into it:

**`secrets\kafka_server_jaas.conf`** - the broker's own SASL/PLAIN identities:

```
KafkaServer {
   org.apache.kafka.common.security.plain.PlainLoginModule required
   username="broker"
   password="broker-secret"
   user_broker="broker-secret"
   user_kafkaclient="kafkaclient-secret";
};
```

**`secrets\client.properties`** - used by `kafka-topics`/`kafka-console-producer`/
`kafka-console-consumer` (they connect to the same credential-protected listener as any
other client):

```
security.protocol=SASL_PLAINTEXT
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="kafkaclient" password="kafkaclient-secret";
```

**`secrets\schema-registry.jaas`** - points the registry at its password file:

```
SchemaRegistry-Props {
   org.eclipse.jetty.jaas.spi.PropertyFileLoginModule required
   file="/etc/schema-registry/secrets/schema_registry.password"
   debug="false";
};
```

**`secrets\schema_registry.password`** - Jetty's `username: password,role` format:

```
schemaregistry: schemaregistry-secret,admin
```

(`setup-podman-kafka.bat` generates all four of these for you from
[config/credentials.json](config/credentials.json) - see
[Configuring credentials](#configuring-credentials) above - the manual step above just shows
what it writes.)

### 2. Create a network

```bat
podman network create iis-wms-kafka-net
```

### 3. Start the Kafka broker (KRaft, single node, SASL/PLAIN)

```bat
podman run -d --name iis-wms-kafka --network iis-wms-kafka-net -p 9092:9092 -v "%CD%\secrets:/etc/kafka/secrets:Z" -e KAFKA_OPTS=-Djava.security.auth.login.config=/etc/kafka/secrets/kafka_server_jaas.conf -e KAFKA_NODE_ID=1 -e CLUSTER_ID=MkU3OEVBNTcwNTJENDM2Qk -e KAFKA_PROCESS_ROLES=broker,controller -e KAFKA_LISTENERS=PLAINTEXT://0.0.0.0:29092,CONTROLLER://0.0.0.0:29093,SASL_PLAINTEXT_HOST://0.0.0.0:9092,SASL_PLAINTEXT_NET://0.0.0.0:9093 -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://iis-wms-kafka:29092,SASL_PLAINTEXT_HOST://localhost:9092,SASL_PLAINTEXT_NET://iis-wms-kafka:9093 -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,SASL_PLAINTEXT_HOST:SASL_PLAINTEXT,SASL_PLAINTEXT_NET:SASL_PLAINTEXT -e KAFKA_SASL_ENABLED_MECHANISMS=PLAIN -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@iis-wms-kafka:29093 -e KAFKA_INTER_BROKER_LISTENER_NAME=PLAINTEXT -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 -e KAFKA_AUTO_CREATE_TOPICS_ENABLE=true confluentinc/cp-kafka:7.6.0
```

`CLUSTER_ID` is Confluent's own documented example KRaft cluster id - any fixed, valid
base64 UUID works for a throwaway local broker; it just has to be present.

Three listeners are advertised: `PLAINTEXT` (`iis-wms-kafka:29092`, unauthenticated,
for containers on the same Podman network, e.g. Schema Registry reading committed
offsets), `SASL_PLAINTEXT_HOST` (`localhost:9092`, credential-protected, for the app
and CLI tools), and `SASL_PLAINTEXT_NET` (`iis-wms-kafka:9093`, also credential-protected,
in-network only - for Kafka UI and Kafka REST Proxy, see steps 4a/4b). There's only one broker, so the
unauthenticated `PLAINTEXT` listener is never reachable from outside the Podman network -
only `SASL_PLAINTEXT_HOST` is published to the host, and `SASL_PLAINTEXT_NET` isn't
published at all (nothing outside the network needs it).

`SASL_PLAINTEXT_HOST` and `SASL_PLAINTEXT_NET` can't be merged into one listener: a
listener's advertised address is fixed and returned to every client that connects to it,
regardless of how they reached it - `SASL_PLAINTEXT_HOST` always tells clients to
(re)connect to `localhost:9092`, which is correct for the app/CLI tools but wrong for a
sibling container like Kafka UI, where "localhost" means its own loopback, not the
broker.

### 4. Start the Schema Registry (HTTP Basic Auth)

```bat
podman run -d --name iis-wms-schema-registry --network iis-wms-kafka-net -p 8085:8081 -v "%CD%\secrets:/etc/schema-registry/secrets:Z" -e SCHEMA_REGISTRY_OPTS=-Djava.security.auth.login.config=/etc/schema-registry/secrets/schema-registry.jaas -e SCHEMA_REGISTRY_AUTHENTICATION_METHOD=BASIC -e SCHEMA_REGISTRY_AUTHENTICATION_ROLES=admin -e SCHEMA_REGISTRY_AUTHENTICATION_REALM=SchemaRegistry-Props -e SCHEMA_REGISTRY_HOST_NAME=iis-wms-schema-registry -e SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS=PLAINTEXT://iis-wms-kafka:29092 -e SCHEMA_REGISTRY_LISTENERS=http://0.0.0.0:8081 confluentinc/cp-schema-registry:7.6.0
```

Container port `8081` is published as host port `8085`, so it lines up with
`Kafka:SchemaRegistryUrl=http://localhost:8085`. The registry talks to the broker over
the unauthenticated in-network `PLAINTEXT` listener - only requests from *outside* to
the registry's own REST API need the Basic Auth credentials.

### 4a. Start Kafka UI

Write `secrets\kafka-ui.env` (one `KEY=value` per line - a `--env-file`, not `-e` flags,
since `KAFKA_CLUSTERS_0_PROPERTIES_SASL_JAAS_CONFIG`'s value has embedded quotes/spaces/a
semicolon, the same escaping hazard the JAAS conf files in step 1 avoid):

```
KAFKA_CLUSTERS_0_NAME=iis-wms-local
KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS=iis-wms-kafka:9093
KAFKA_CLUSTERS_0_PROPERTIES_SECURITY_PROTOCOL=SASL_PLAINTEXT
KAFKA_CLUSTERS_0_PROPERTIES_SASL_MECHANISM=PLAIN
KAFKA_CLUSTERS_0_PROPERTIES_SASL_JAAS_CONFIG=org.apache.kafka.common.security.plain.PlainLoginModule required username="kafkaclient" password="kafkaclient-secret";
KAFKA_CLUSTERS_0_SCHEMAREGISTRY=http://iis-wms-schema-registry:8081
KAFKA_CLUSTERS_0_SCHEMAREGISTRYAUTH_USERNAME=schemaregistry
KAFKA_CLUSTERS_0_SCHEMAREGISTRYAUTH_PASSWORD=schemaregistry-secret
```

Then start it:

```bat
podman run -d --name iis-wms-kafka-ui --network iis-wms-kafka-net -p 8090:8080 --env-file "%CD%\secrets\kafka-ui.env" docker.io/kafbat/kafka-ui:v1.5.0
```

`KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS` uses the `SASL_PLAINTEXT_NET` listener
(`iis-wms-kafka:9093`) from step 3, not `localhost:9092` - see that step's explanation.
`KAFKA_CLUSTERS_0_SCHEMAREGISTRY` uses the registry's container-network address
(`iis-wms-schema-registry:8081`), not the host-published `localhost:8085` - no reason to
leave the Podman network for a container-to-container call. Browse to
**http://localhost:8090** once it's up.

### 4b. Start Kafka REST Proxy

Write `secrets\kafka-rest.env`, same reasoning as `kafka-ui.env` above (a
`--env-file`, since `KAFKA_REST_CLIENT_SASL_JAAS_CONFIG` has the same embedded
quotes/spaces/semicolon):

```
KAFKA_REST_HOST_NAME=iis-wms-kafka-rest
KAFKA_REST_BOOTSTRAP_SERVERS=iis-wms-kafka:9093
KAFKA_REST_CLIENT_SECURITY_PROTOCOL=SASL_PLAINTEXT
KAFKA_REST_CLIENT_SASL_MECHANISM=PLAIN
KAFKA_REST_CLIENT_SASL_JAAS_CONFIG=org.apache.kafka.common.security.plain.PlainLoginModule required username="kafkaclient" password="kafkaclient-secret";
KAFKA_REST_SCHEMA_REGISTRY_URL=http://iis-wms-schema-registry:8081
KAFKA_REST_SCHEMA_REGISTRY_BASIC_AUTH_CREDENTIALS_SOURCE=USER_INFO
KAFKA_REST_SCHEMA_REGISTRY_BASIC_AUTH_USER_INFO=schemaregistry:schemaregistry-secret
KAFKA_REST_LISTENERS=http://0.0.0.0:8082
```

Then start it:

```bat
podman run -d --name iis-wms-kafka-rest --network iis-wms-kafka-net -p 8086:8082 --env-file "%CD%\secrets\kafka-rest.env" confluentinc/cp-kafka-rest:8.2.2
```

Same `SASL_PLAINTEXT_NET` listener as Kafka UI, for the same reason (step 3's explanation).
Browse/curl **http://localhost:8086** once it's up - see "Push a message via curl" above
for the actual produce command.

### 5. Wait for the broker to be ready

```bat
podman exec iis-wms-kafka kafka-topics --bootstrap-server localhost:9092 --command-config /etc/kafka/secrets/client.properties --list
```

Run this until it returns without an error (a fresh broker can take a few seconds).
`--command-config` is required now - the broker's client listener demands SASL/PLAIN
credentials for every connection, including from tools run inside the same container.

### 6. Create the topic

```bat
podman exec iis-wms-kafka kafka-topics --bootstrap-server localhost:9092 --command-config /etc/kafka/secrets/client.properties --create --if-not-exists --topic inventory-events --partitions 1 --replication-factor 1
```

Or run [registration/register-defaults.bat](registration/register-defaults.bat) instead of this one `kafka-topics`
call - it also creates `inventory-bulk-import`, pre-creates the app's default consumer
groups, and registers the Avro schema (see "Registering defaults on their own" above).

### 7. Push a message

The consumer for this topic
([KafkaConsumerHostedService](../../src/Infrastructure/IIS.WMS.Consumer.Infrastructure/Messaging/Kafka/KafkaConsumerHostedService.cs))
deserializes each message body as
[InboundInventoryEventMessage](../../src/Infrastructure/IIS.WMS.Consumer.Infrastructure/Messaging/InboundInventoryEventMessage.cs):
`EventId`, `WarehouseId`, `Sku`, `Quantity`, `EventType`.

Write a message to a file, then pipe it into the console producer:

```bat
echo {"EventId":"evt-001","WarehouseId":"WH1","Sku":"SKU-123","Quantity":10,"EventType":"Create"}> "%TEMP%\message.json"
podman exec -i iis-wms-kafka kafka-console-producer --bootstrap-server localhost:9092 --producer.config /etc/kafka/secrets/client.properties --topic inventory-events < "%TEMP%\message.json"
```

`EventId` should be unique per message - the consumer uses it as the deduplication /
Service Bus message id.

No Kafka headers (`Correlation-Id`, `Deduplication-Id`, `Type`) are sent above; all three
are optional and degrade gracefully when absent (see
[KafkaConsumerHostedServiceBase.cs](../../src/Infrastructure/IIS.WMS.Consumer.Infrastructure/Messaging/Kafka/KafkaConsumerHostedServiceBase.cs)) -
a fresh correlation id is generated, dedup is skipped, and the default schema handler is used.

### 8. Verify the message landed, into a consumer group (optional)

```bat
podman exec iis-wms-kafka kafka-console-consumer --bootstrap-server localhost:9092 --consumer.config /etc/kafka/secrets/client.properties --topic inventory-events --group inventory-events-consumer --from-beginning --max-messages 1
```

`--group inventory-events-consumer` matches `appsettings.json`'s actual
`Kafka:ConsumerGroup` for this topic and makes the consumed message's offset show up
against that group in Kafka UI - omit `--group` and Kafka's console consumer instead joins
a random one-off group that won't mean anything to look at afterwards.

### 9. Verify Schema Registry auth (optional)

```bat
curl -u schemaregistry:schemaregistry-secret http://localhost:8085/subjects
curl http://localhost:8085/subjects
```

The first should succeed; the second (no credentials) should get a `401`.

## Cleanup

```bat
podman rm -f iis-wms-kafka iis-wms-schema-registry iis-wms-kafka-ui iis-wms-kafka-rest
podman network rm iis-wms-kafka-net
```

This removes the containers and network but **not** the `iis-wms-kafka-data` named volume
(topics/messages/consumer offsets/registered schemas) - that's by design, so the next
`setup-podman-kafka.bat` run picks up where you left off instead of starting empty. To
also wipe the broker's data and start with a genuinely clean slate:

```bat
podman volume rm iis-wms-kafka-data
```

The generated `secrets\` folder is safe to delete too (or leave - it only ever holds
local, throwaway credentials, never the real Confluent Cloud ones from
`appsettings.json`).

## Troubleshooting

- **`podman: command not found` / version check fails** - Podman Desktop isn't installed,
  or its machine isn't started. Open Podman Desktop and confirm the machine shows as running.
- **Port already in use (`9092` or `8085`)** - something else on your machine is bound to
  that port (another Kafka, a previous run that didn't get cleaned up). Run the Cleanup
  commands above, or change the `-p` mapping.
- **`kafka-topics --list` keeps failing / `SaslAuthenticationException`** - check
  `podman logs iis-wms-kafka`. `setup-podman-kafka.bat` always removes and recreates this
  container itself now, so a credentials/config change always takes effect on the next
  run - on the `kube play`/custom-image paths, or if you edited a credential file by hand,
  remove it yourself first (`podman rm -f iis-wms-kafka`) so the new JAAS file actually
  takes effect - editing the file alone doesn't restart an already-running container.
  Otherwise, check for a typo in `client.properties`/`kafka_server_jaas.conf`.
- **Schema Registry returns `401` even with the right credentials, or won't start** -
  check `podman logs iis-wms-schema-registry`; verify `secrets\schema_registry.password`
  has no trailing spaces around the `:` and `,` (Jetty's `PropertyFileLoginModule` format
  is strict: `username: password,role`).
- **`-v` bind mount fails or the container can't read `/etc/kafka/secrets`** - Podman
  Desktop on Windows translates host paths automatically, but if the mount is rejected,
  try dropping the trailing `:Z` from the `-v` flag (it's an SELinux relabeling hint,
  harmless to omit on Windows).
- **App can't reach `localhost:9092` from inside a container/Pod** - `localhost` only
  resolves to the *same* container/Pod's own network namespace. If the app itself is also
  running as a Podman/Kubernetes Pod (not directly on your machine), use
  `host.containers.internal:9092` instead - see the note in
  [k8s/kafka-consumer/configmap.yaml](../../k8s/kafka-consumer/configmap.yaml).
- **Kafka UI (`http://localhost:8090`) shows the cluster offline, or won't load at all** -
  first check `podman ps` actually shows `iis-wms-kafka-ui` running and
  `podman logs iis-wms-kafka-ui` for the reason. Common causes: it started before the
  broker was ready (it retries in the background - give it 10-20 seconds and refresh), the
  broker's `SASL_PLAINTEXT_NET` listener (port 9093) isn't in
  `KAFKA_LISTENERS`/`KAFKA_ADVERTISED_LISTENERS`/`KAFKA_LISTENER_SECURITY_PROTOCOL_MAP` -
  `setup-podman-kafka.bat` always removes and recreates `iis-wms-kafka` itself now, so this
  shouldn't happen on that path; on `kube play`/custom-image setups, or a hand-edited
  listener config, remove the stale container yourself (`podman rm -f iis-wms-kafka`) and
  re-run to pick up the current definition - or `secrets\kafka-ui.env` has a typo in the
  credentials (must match
  `kafka_server_jaas.conf`'s `user_<username>` entry and
  `schema_registry.password`'s line exactly).
- **`curl` to `http://localhost:8086` (Kafka REST Proxy) hangs, connection-refuses, or
  returns a 500 with a Kafka-side error** - check `podman ps`/`podman logs
  iis-wms-kafka-rest` first. `setup-podman-kafka.bat` always removes and recreates both
  this container and the broker itself now (see "Producing a real InventoryStateChanged
  event" above and the Kafka broker section below), so a stale container/listener config
  is no longer the cause on that path - on `kube play`/custom-image setups, or a
  hand-edited listener config, remove the stale container yourself and re-run. It can also
  take 10-20 seconds after startup before the REST Proxy is actually accepting requests. A
  `415`/`406` response usually means the `Content-Type: application/vnd.kafka.json.v2+json`
  header was left off the `curl` call.
