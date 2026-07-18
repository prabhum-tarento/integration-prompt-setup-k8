# Local Cosmos DB, Service Bus, Azurite, and an Azure CLI tools image, via Podman

Runs the Azure Cosmos DB Linux Emulator (vNext), the Azure Service Bus emulator (plus its required
SQL Server Linux dependency), and Azurite (Blob Storage + Table Storage) under Podman Desktop,
matching this app's local-dev configuration shape - `CosmosDb:AccountEndpoint`/`EmulatorKey`,
`ServiceBus:ConnectionString`/`QueueName`/`BulkInventoryImport:QueueName`, and
`BlobStorage:Hot/Cold:AccountUri` in
[appsettings.Development.json](../../src/Api/IIS.WMS.Consumer.Api/appsettings.Development.json) -
per [cosmos-db.instructions.md](../../docs/ai/cosmos-db.instructions.md) §1 and
[integration-resiliency.instructions.md](../../docs/ai/integration-resiliency.instructions.md) §2/§5/§9.
This is a separate folder/network from [scripts/local-kafka](../local-kafka), since Kafka is a
distinct dependency with its own credentialed setup.

Also builds and starts a fourth container from
[image/tools.Containerfile](image/tools.Containerfile) with the **Azure CLI** installed, on the
same Podman network - see that file's header comment and "The Azure CLI tools container" section
below for exactly what it is (and isn't) useful for here, including provisioning Azurite's Blob
containers/tables.

| Setting | Value |
|---|---|
| `CosmosDb:AccountEndpoint` | `https://localhost:8081` (default - see [Configuring ports](#configuring-ports) if you've remapped it) |
| `CosmosDb:EmulatorKey` | `emulatorKey` in [config/cosmos-db-config.json](config/cosmos-db-config.json) - the well-known, publicly documented emulator master key by default (see script output for the actual value in effect - not a secret specific to this install, but still read from user-secrets, never `appsettings.json`, for consistency with every other local credential in this repo) |
| Cosmos DB Data Explorer | `http://localhost:1234` (default) |
| `ServiceBus:ConnectionString` | `Endpoint=sb://localhost;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true;` (default AMQP port 5672, implicit; `SharedAccessKeyName`/`SharedAccessKey` from `config/service-bus-config.json`'s `Credentials` key - see [Configuring credentials](#configuring-credentials)) |
| `ServiceBus:QueueName` | `inventory-events` (session-enabled) |
| `ServiceBus:BulkInventoryImport:QueueName` | `inventory-bulk-import` (non-session) |
| Other queues declared in [config/service-bus-config.json](config/service-bus-config.json) | `inventory-state-changed`, `inventory-adjusted` (both session-enabled - Kafka relay routing targets, see integration-resiliency.instructions.md §1) |
| Service Bus emulator management/health port | `5300` default (Administration Client calls need this port appended to the connection string above - see "Interact with the emulator" below) |
| Azurite accounts | `iiswmshotstore` (Hot tier), `iiswmscoldstore` (Cold tier) - two genuinely separate custom accounts on one Azurite instance, declared in [config/storage-config.json](config/storage-config.json) - see [Azurite (Blob Storage + Table Storage) specifics](#azurite-blob-storage--table-storage-specifics) |
| `BlobStorage:Hot:AccountUri` / `BlobStorage:Cold:AccountUri` | explicit `DefaultEndpointsProtocol=http;AccountName=...;AccountKey=...;BlobEndpoint=...;TableEndpoint=...;` per account - printed in full by `setup-storage-data.ps1`'s own console output at the end of a run |
| Blob containers | Hot: `imports`, `exports`, `consumer-dead-letter`; Cold: `request-audit` - provisioned per-account from [config/storage-config.json](config/storage-config.json) |
| Table Storage | endpoint available per account, but **no tables exist yet** - nothing in this codebase uses Table Storage currently |
| Browse/upload/download Blob containers in a browser | `http://localhost:5572` (default), interactive rclone web UI - see [Interactive web UI (rclone)](#interactive-web-ui-rclone) |
| rclone web UI login | `username`/`password` in [config/storage-config.json](config/storage-config.json)'s `rcloneUi` key - see [Configuring credentials](#configuring-credentials) |
| Azure CLI tools container | `podman exec -it iis-wms-emulators-tools az ...` |

All eight ports above are defaults from [../ports.env](../ports.env) - see
[Configuring ports](#configuring-ports) to change any of them.

## Prerequisites

- [Podman Desktop](https://podman-desktop.io/) installed, with its Podman machine started.
- [curl](https://curl.se/), used by the script's own readiness checks (ships with Windows 10+ by
  default).
- Outbound internet access the first time you run this, to pull
  `mcr.microsoft.com/cosmosdb/linux/azure-cosmos-emulator:vnext-latest`,
  `mcr.microsoft.com/azure-messaging/servicebus-emulator:latest`,
  `mcr.microsoft.com/mssql/server:2022-latest`, `mcr.microsoft.com/azure-storage/azurite:3.36.0`,
  `docker.io/rclone/rclone:1.74.4`, and to build the `mcr.microsoft.com/azure-cli:2.88.0-azurelinux3.0`-based
  tools image. The rclone web UI container also downloads its GUI assets from GitHub
  (`rclone/rclone-webui-react`) the first time it starts.
- **Minimum hardware for the Service Bus emulator + SQL Server Linux pair**: 2 GB RAM, 5 GB disk
  (Microsoft's own stated minimum).

## License terms you're accepting

The Service Bus emulator and its SQL Server Linux dependency each require accepting their own
Microsoft license terms before they'll run - this is baked into both containers as `ACCEPT_EULA=Y`.
The script will **not** set that silently: it requires an explicit `-AcceptEula` flag (or
`ACCEPT_EULA=Y` in your environment) before starting anything, and prints these two links if you
haven't:

- [Service Bus emulator EULA](https://github.com/Azure/azure-service-bus-emulator-installer/blob/main/EMULATOR_EULA.txt)
- [SQL Server Linux EULA](https://go.microsoft.com/fwlink/?LinkId=746388)

To start this alongside Kafka in one step, see
[scripts\setup-podman-local-stack.bat](../setup-podman-local-stack.bat) instead of running the
command below directly.

## Quick start

```bat
scripts\local-emulators\setup-podman-emulators.bat -AcceptEula
```

Starts, in order: the Cosmos DB Emulator (waits on its own `/ready` health probe), SQL Server Linux,
the Service Bus emulator (waits on its own `/health` endpoint, reading
[config/service-bus-config.json](config/service-bus-config.json) for the four queues above),
[get-azurite-accounts-env.ps1](get-azurite-accounts-env.ps1) (reads the account list from
[config/storage-config.json](config/storage-config.json)), Azurite itself (started with that account
list, then waits for its Blob/Table listeners to answer), the Azure CLI tools container,
[setup-storage-data.ps1](setup-storage-data.ps1) (provisions each account's Blob containers/tables via
that tools container - see "Azurite (Blob Storage + Table Storage) specifics" below), and finally the
rclone web UI container (see [Interactive web UI (rclone)](#interactive-web-ui-rclone)). Every
container is removed and recreated fresh on every run (same reasoning as `setup-podman-kafka.bat`:
guarantees the current `Config.json`/image is always picked up instead of silently continuing on a
stale one) - unlike that script, nothing here uses a named Podman volume, since none of these five
emulators/tools need data to persist across a re-run for local manual testing the way the Kafka
broker's topic/offset history does.

## Configuring ports

All host-side ports this script publishes - the Cosmos DB Emulator's endpoint, health probe, and
Data Explorer, the Service Bus emulator's AMQP and management ports, and Azurite's Blob and Table
ports - come from [../ports.env](../ports.env), a plain `KEY=VALUE` file (one setting per
line, `#` for comments) **shared with** [../local-kafka/setup-podman-kafka.bat](../local-kafka/setup-podman-kafka.bat)
(see [../local-kafka/README.md](../local-kafka/README.md)'s own "Configuring ports" section for that
script's keys in the same file):

```
COSMOS_ENDPOINT_PORT=8081
COSMOS_HEALTH_PORT=8080
COSMOS_EXPLORER_PORT=1234
SERVICEBUS_AMQP_PORT=5672
SERVICEBUS_MGMT_PORT=5300
AZURITE_BLOB_PORT=10000
AZURITE_TABLE_PORT=10002
RCLONE_UI_PORT=5572
```

Edit a value there and re-run `setup-podman-emulators.bat` to remap it - useful when a default
already conflicts with something else already running on your machine (another local Cosmos/Service
Bus emulator, a different container stack, IIS Express, etc). The script falls back to the defaults
above for any key that's missing or if the file itself is absent, so `ports.env` only needs to
declare the values you actually want to change.

This file lives one level up, under `scripts\`, rather than in this folder's own `config\` next to
`cosmos-db-config.json`/`service-bus-config.json` - it's a plain `KEY=VALUE` file (read directly by
`setup-podman-emulators.bat` itself, a `.bat` file with no JSON parser available, unlike this
folder's JSON config files) shared with the sibling `local-kafka` stack's own script, so one file
covers every port both scripts publish.

Only the **host-side** mapping is configurable - each container's own internal port (what the
emulator listens on inside its network namespace) stays fixed, so this only ever helps with port
conflicts on your machine, not anything containers-to-container.

**Changing a port here does not update `appsettings.Development.json` or your user-secrets** - if
you move `CosmosDb:AccountEndpoint`/`ServiceBus:ConnectionString`/`BlobStorage:Hot/Cold:AccountUri`,
etc. onto a non-default port, update those to match yourself, or the app will keep pointing at the
old one. `BlobStorage:Hot/Cold:AccountUri`'s connection strings are always the explicit
`BlobEndpoint=...;TableEndpoint=...;` form (never the `UseDevelopmentStorage=true` shorthand - see
"Azurite (Blob Storage + Table Storage) specifics" below for why), so remapping `AZURITE_BLOB_PORT`/
`AZURITE_TABLE_PORT` always means updating both values there. The script's own console output at the
end of a run (specifically, `setup-storage-data.ps1`'s per-account summary) always reflects the ports
actually in effect, so treat that as the source of truth if you're unsure what to put in
`appsettings.Development.json`.

## Configuring credentials

The SQL Server Linux SA password and the Service Bus emulator's own
`SharedAccessKeyName`/`SharedAccessKey` come from a `Credentials` key added alongside
`UserConfig` in [config/service-bus-config.json](config/service-bus-config.json) - the same
file already bind-mounted into the Service Bus emulator container as its own `Config.json` (see
"Azure Service Bus emulator" below), since both credentials only ever matter to that
container/its SQL dependency:

```json
{
  "UserConfig": { "...": "see the file itself for the full queue declarations" },
  "Credentials": {
    "Sql": {
      "SaPassword": "L0cal-Sb-Emul4tor!"
    },
    "ServiceBus": {
      "SharedAccessKeyName": "RootManageSharedAccessKey",
      "SharedAccessKey": "SAS_KEY_VALUE"
    }
  }
}
```

`Credentials` is a sibling of `UserConfig`, not nested inside it - the emulator's own config
loader only reads `UserConfig` and ignores unrecognized top-level properties, so this rides
along in the same file without the emulator itself ever seeing or validating it. Edit a value
and re-run `setup-podman-emulators.bat` to pick it up - both the SQL Server Linux and Service
Bus emulator containers are always removed and recreated fresh on every run (see "Quick start"
above), so a credential change always takes effect. The script falls back to the defaults shown
above for any key that's missing or if the file itself is absent, so `Credentials` only needs
to declare the values you actually want to change.

**`Sql:SaPassword` must meet SQL Server's own complexity policy** (8+ characters, with
upper-case, lower-case, digit, and symbol) if you change it - the SQL Server Linux container
will fail to start otherwise (check `podman logs iis-wms-emulators-mssql`).

**`ServiceBus:SharedAccessKeyName`/`SharedAccessKey` are pure display values, not enforced by
the emulator at all** - `UseDevelopmentEmulator=true` bypasses real SAS signature checks (see
"Service Bus emulator specifics" below), so changing them only changes what this script's own
console output prints as `ServiceBus:ConnectionString`, not any actual authorization. There's
no need to change them unless you want the printed connection string to match a specific value
for some other reason.

Like [../ports.env](../ports.env), `Credentials` holds only local, throwaway values - never a
value derived from a real Azure resource - so it's safe to commit and safe to edit freely.

**The rclone web UI's own login** comes from a separate `rcloneUi` key in
[config/storage-config.json](config/storage-config.json) (a sibling of that file's `accounts` array,
not the `service-bus-config.json` file above - this credential only ever matters to that one
container):

```json
{
  "accounts": [ "...": "see the file itself for the full account declarations" ],
  "rcloneUi": {
    "username": "rcloneui",
    "password": "Rcl0ne-Ui-Local!"
  }
}
```

This is **not** an Azurite/Azure Storage credential at all - it only gates the rclone web UI
container's own RC endpoint (`--rc-user`/`--rc-pass`), which would otherwise serve with no login.
Edit a value and re-run `setup-podman-emulators.bat` to pick it up - the rclone-ui container is
always removed and recreated fresh on every run (see "Quick start" above), so a credential change
always takes effect. Same local-only, throwaway, safe-to-commit reasoning as every other credential
here - falls back to the defaults shown above if the key/file is missing.

### Cosmos DB Emulator specifics

- `--protocol https` is required, not optional - the `Microsoft.Azure.Cosmos` SDK this repo uses
  (cosmos-db.instructions.md) doesn't support talking to the emulator over plain HTTP, unlike the
  image's own HTTP-by-default behavior.
- Port `8081` (default) is the Cosmos endpoint itself; `8080` (default) is the emulator's own
  health-probe endpoint (`/alive`, `/ready`, `/status` - what the script's wait loop polls, not
  something the app calls); `1234` (default) is the bundled Data Explorer UI. All three are host-side
  port mappings only (the container's own internal ports are fixed) - see
  [Configuring ports](#configuring-ports) to remap any of them.
- **Data does not persist across a re-run** of this script (the container is always recreated). If
  you want seeded data to survive across host restarts (not just this script's own re-run), bind-mount
  a host folder at `/data` yourself and add `ENABLE_INIT_DATA=true` - see
  [Microsoft's own "Persist data across restarts"](https://learn.microsoft.com/azure/cosmos-db/emulator-linux#persist-data-across-restarts)
  section; not wired into this script, since the point of always-fresh containers here is
  config/image freshness, not preserving prior runs' documents. Instead, every run reprovisions the
  database/containers and reseeds them from [data](data) - see below.

### Database, containers, and seed data

Once the Cosmos DB Emulator reports ready, the script runs
[setup-cosmos-data.ps1](setup-cosmos-data.ps1), which:

1. Creates the database named by `databaseName` in
   [config/cosmos-db-config.json](config/cosmos-db-config.json) - currently `InventoryDb` (matching
   `CosmosDb:DatabaseName`) - if it doesn't already exist.
2. Creates the containers listed in that same file's `containers` array - currently
   `InventoryEvents`, `OrderArchive`, `BulkInventoryImports` (matching the `ContainerName` const in
   each repository under
   `src/Infrastructure/IIS.WMS.Consumer.Infrastructure/Persistence/CosmosDb/Repository`) - every
   container sharing the single `partitionKey` path also declared in that file (default `/category`,
   matching `CosmosDb:PartitionKeyPath` in `appsettings.Development.json`).
3. Upserts every document from [data/InventoryEvents.json](data/InventoryEvents.json),
   [data/OrderArchive.json](data/OrderArchive.json), and
   [data/BulkInventoryImports.json](data/BulkInventoryImports.json) into the matching container - one
   JSON array per file, file name (without extension) is the container name. A document with no value
   at the configured `partitionKey` path upserts under Cosmos's "Undefined" partition (sent on the
   wire as `x-ms-documentdb-partitionkey: [{}]`) rather than failing.

**`emulatorKey`** (also in `cosmos-db-config.json`) is the master key this script signs its Cosmos DB
REST calls with, and the same value `setup-podman-emulators.bat` prints as `CosmosDb:EmulatorKey` and
passes to this script via `-Key` - one config value, wired through both places, rather than two
independently hardcoded copies of the same literal. Defaults to Microsoft's well-known, publicly
documented emulator key (the same fixed value on every machine running this image, not a secret
specific to this install); edit it here (or pass `-Key` directly) if you need this local Cosmos DB
Emulator to accept a different key for some reason. Precedence is explicit `-Key` param > this file's
`emulatorKey` > a built-in fallback to that same well-known value, for a bare invocation with no
config file wired up at all - same precedence `-DatabaseName`/`-PartitionKey` already use.

**`partitionKey`'s default of `/category` matches every document's actual shape**:
`OrderArchiveDocument`, `AuditEntryDocument`, `InventoryEventDocument`, and
`InventoryBulkImportItemDocument` all carry a `Category` property (cosmos-db.instructions.md §4), so
one shared `partitionKey` value works for every container in `cosmos-db-config.json`, not just
`OrderArchive`. For `InventoryEvents`/`BulkInventoryImports`, the seed data's `category` value is
still the composite `WarehouseId:Sku` (see `data/InventoryEvents.json`), not a low-cardinality
category string - only the property name is shared across entities, not the value's shape (§4).

It talks directly to the Cosmos DB REST API (signed with `emulatorKey` above, well-known by default),
not the `Microsoft.Azure.Cosmos` SDK, so it stays a standalone script with no new project/NuGet
dependency.
Safe to re-run: database/container creation tolerates a `409` (already exists), and every document
upserts rather than inserts, so editing a `data\*.json` file and re-running the script (or the whole
`setup-podman-emulators.bat`) updates that file's documents in place. **Changing `partitionKey` does
require deleting the existing containers first** - a container's partition key path is immutable once
created, so the script's `409`-tolerant "already exists" path leaves a pre-existing container on its
original partition key even if you later change the config.

To add a new container, add its name to `cosmos-db-config.json`'s `containers` array and a same-named
`data\<ContainerName>.json` file (a JSON array of documents), then re-run the script - no other wiring
needed. To rename the database, change `databaseName` in the same file (and `CosmosDb:DatabaseName` in
`appsettings.Development.json`, so the app points at it too).

**A note on `$PSScriptRoot` in this script**: `DataDir`/`ConfigDir` deliberately default in the script
*body*, not in the `param()` block. Referencing `$PSScriptRoot` directly in a `param()` default value
on a script that declares `[CmdletBinding()]` reads back empty (reproduced against Windows PowerShell
5.1 - a parameter-binding-order quirk, not something specific to how this script gets invoked), which
previously surfaced as `Join-Path : Cannot bind argument to parameter 'Path' because it is an empty
string.` the first time this ran nested inside another PowerShell process (exactly how
`setup-podman-emulators.bat` invokes it). If you add another path-shaped parameter here, default it in
the body the same way, not in `param()`.

To reprovision/reseed without restarting the containers themselves:

```bat
powershell -NoProfile -ExecutionPolicy Bypass -File scripts\local-emulators\setup-cosmos-data.ps1
```

### Service Bus emulator specifics

- **Connection string**: `Endpoint=sb://localhost;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true;`
  (both defaults, from `config/service-bus-config.json`'s `Credentials` key - see
  [Configuring credentials](#configuring-credentials) above) when the app runs directly on your host
  (not itself containerized) - `SharedAccessKey` is a literal placeholder token the emulator doesn't
  actually validate (`UseDevelopmentEmulator=true` bypasses real SAS signature checks), not a value
  you need to generate. The `sb://localhost` form with no
  port implies the emulator's default AMQP port, `5672` - if `../ports.env`'s
  `SERVICEBUS_AMQP_PORT` has been remapped away from that default, append it explicitly instead
  (`sb://localhost:<port>`; the script's own console output does this for you). If the app itself
  later runs containerized on this same Podman network, use
  `Endpoint=sb://iis-wms-servicebus-emulator;...` instead - see
  [Microsoft's "Choosing the right connection string"](https://learn.microsoft.com/azure/service-bus-messaging/test-locally-with-service-bus-emulator#choosing-the-right-connection-string)
  for the other host-value variants (different machine same network, different bridge network, etc).
- **Administration Client** operations (`ServiceBusAdministrationClient`, used by
  `MessagingServiceCollectionExtensions` to build the admin client this repo registers) need the
  management port appended explicitly: `Endpoint=sb://localhost:5300;...` (`5300` is the default from
  `../ports.env`'s `SERVICEBUS_MGMT_PORT`) - the plain `sb://localhost;...` form above is for the
  data-plane `ServiceBusClient` only.
- `config/service-bus-config.json` declares this app's four queues - `inventory-events`,
  `inventory-state-changed`, `inventory-adjusted` (all three `RequiresSession: true`, matching the
  `SessionId = {WarehouseId}:{Sku}` the Kafka relay sets on every message it publishes, per
  integration-resiliency.instructions.md §1/§2) and `inventory-bulk-import` (`RequiresSession: false`,
  matching the separate non-session bulk-import consumer). `RequiresDuplicateDetection` is `false`
  on all four - this app's own Nexus-backed dedup check (`IDeduplicationService`) is the mechanism in
  use, not Service Bus's built-in duplicate detection window.
- The SQL Server Linux container (`iis-wms-emulators-mssql`) is purely the Service Bus emulator's
  internal metadata store - it holds nothing this app or its tests ever query directly.

### Azurite (Blob Storage + Table Storage) specifics

- **Image tag is pinned** (`mcr.microsoft.com/azure-storage/azurite:3.36.0`), unlike the Cosmos DB
  Emulator/Service Bus emulator images above - Azurite publishes numbered release tags (see
  [Azure/Azurite's README.mcr.md](https://github.com/Azure/Azurite/blob/main/README.mcr.md)), so
  there's no need for the `vnext-latest`/`latest` exception those two document for themselves.
- **No EULA gate**: Azurite is MIT-licensed - nothing to accept, unlike the Service Bus
  emulator/SQL Server Linux pair.
- Ports `10000` (Blob, default) and `10002` (Table, default) are published; Azurite's third service,
  Queue Storage (port `10001` internally), runs inside the container regardless (there's no
  combined "Blob + Table only" binary - only the full `azurite` binary, used here, or the
  single-service `azurite-blob`/`azurite-queue`/`azurite-table` ones) but is neither published nor
  used, since nothing in this app talks to Queue Storage.
- `--blobHost 0.0.0.0 --tableHost 0.0.0.0` (not Azurite's own `127.0.0.1` default) are required for
  Podman's port publishing to actually reach the listener from outside the container's network
  namespace.
- **No dedicated health-probe endpoint** like the other two emulators - the wait loop instead treats
  any HTTP response at all (even Azurite's own `400`/`401` for an unauthenticated/malformed request)
  as proof the listener is up, using plain `curl -s` (not `-sf`, which would treat that same response
  as a failure).

#### Multiple accounts (one Azurite instance, one per tier)

- **Hot and Cold are two genuinely separate Storage accounts on this one Azurite instance** -
  `iiswmshotstore` and `iiswmscoldstore` - not one shared account, matching how
  integration-resiliency.instructions.md §5 documents them in every other environment. This uses
  Azurite's own [custom storage accounts & keys
  feature](https://github.com/Azure/Azurite/blob/main/README.md#customizable-storage-accounts--keys)
  (the `AZURITE_ACCOUNTS` environment variable, format `account1:key1;account2:key1;...`) - one
  Azurite process, many independent named accounts, each with its own containers/tables/keys, rather
  than running a second Azurite container per tier.
- **Where the account list comes from**: [config/storage-config.json](config/storage-config.json) -
  an `accounts` array, each entry a `{ name, key, containers, tables }` object. Currently:

  ```json
  {
    "accounts": [
      { "name": "iiswmshotstore",  "containers": ["imports", "exports", "consumer-dead-letter"], "tables": [] },
      { "name": "iiswmscoldstore", "containers": ["request-audit"], "tables": [] }
    ]
  }
  ```

  (`key` omitted above for brevity - see the actual file; both accounts use Azurite's own well-known
  key, the same fixed value on every machine, not a secret specific to this install.) `imports`/
  `exports` are reserved per integration-resiliency.instructions.md §5 but not yet written to by any
  code path; `consumer-dead-letter`/`request-audit` match `BlobStorageOptions`'
  `ConsumerDeadLetterContainerName`/`RequestAuditContainerName` in
  `src/Infrastructure/IIS.WMS.Consumer.Infrastructure/BlobStorage/BlobStorageOptions.cs`. **No tables
  on either account** - nothing in this codebase uses Table Storage yet; add a name to an account's
  `tables` array, and re-run either this script or `setup-storage-data.ps1` directly, once something
  does. To add a whole new account (a third tier, say), add another entry to the `accounts` array -
  no other wiring is needed; both scripts below iterate the array, they don't hardcode account names.
- **`AZURITE_ACCOUNTS` is set once, at Azurite container start** - before
  [get-azurite-accounts-env.ps1](get-azurite-accounts-env.ps1) (a tiny script, separate from
  `setup-storage-data.ps1` below, purely because of this ordering: the env var has to exist *before*
  Azurite starts, while container/table provisioning can only happen *after* both Azurite and the
  tools container are up) reads `storage-config.json` and prints `name:key;name:key;...` for
  `setup-podman-emulators.bat` to capture and pass via `-e`.
- **Setting `AZURITE_ACCOUNTS` at all disables Azurite's own default `devstoreaccount1` account**
  unless it's itself included in the accounts list - this repo's setup deliberately does not include
  it, since every account the app actually needs is already declared by its own name.
- **Containers and tables**: [setup-storage-data.ps1](setup-storage-data.ps1) loops over every
  account in `storage-config.json` and creates that account's own containers/tables via
  `az storage container/table create` - see that script's own header comment for why it execs into
  the tools container rather than hand-rolling Azure Storage's Shared Key/Shared Key Lite signing
  algorithms in PowerShell (the way `setup-cosmos-data.ps1` hand-rolls Cosmos's). Safe to re-run: both
  commands succeed whether or not the resource already exists. At the end of a run it prints each
  account's full connection string (localhost form) - **that output, not this README, is the source
  of truth** for what to put in `appsettings.Development.json`/user-secrets, since it reflects
  whatever `AZURITE_BLOB_PORT`/`AZURITE_TABLE_PORT` are actually in effect.
- **Connection string shape**: always the explicit
  `DefaultEndpointsProtocol=http;AccountName=<name>;AccountKey=<key>;BlobEndpoint=http://localhost:<blobPort>/<name>;TableEndpoint=http://localhost:<tablePort>/<name>;`
  form when the app runs directly on your host (matches `BlobStorage:Hot:AccountUri`/
  `Cold:AccountUri` in `appsettings.Development.json`) - **not** the `UseDevelopmentStorage=true`
  shorthand, since that resolves only to Azurite's single default `devstoreaccount1` account, which
  is disabled here (see above). If the app itself later runs containerized on this same Podman
  network, use the container-name form instead - `BlobEndpoint=http://iis-wms-storage-emulator:10000/<name>;
  TableEndpoint=http://iis-wms-storage-emulator:10002/<name>;...` - same container-name-vs-localhost
  distinction as the Service Bus connection string above (this is also exactly the form
  `setup-storage-data.ps1` itself uses internally, since every `az` call it makes runs inside the
  tools container, a different network namespace than this script's own).

#### Interactive web UI (rclone)

Azurite has no built-in web Data Explorer (unlike the Cosmos DB Emulator above). The setup script
starts an [rclone](https://rclone.org/docker/) container (`iis-wms-rclone-ui`, image
`docker.io/rclone/rclone:1.74.4`) running `rclone rcd --rc-web-gui`, a real interactive web UI - browse,
upload, download, delete, and rename blobs, not just view them. This replaces an earlier attempt at a
more purpose-built option, `ghcr.io/adrianhall/azurite-ui`, which turned out not to be publicly
pullable; rclone is a well-known, actively maintained project whose `azureblob` backend documents
Azurite emulator support directly.

- **URL**: `http://localhost:5572` (default port, [../ports.env](../ports.env)'s `RCLONE_UI_PORT`) -
  also printed by `setup-podman-emulators.bat`'s own console output and shown, with a live up/down
  status, on [scripts/generate-local-stack-summary.ps1](../generate-local-stack-summary.ps1)'s
  generated dashboard page.
- **Login**: username/password from `config/storage-config.json`'s `rcloneUi` key - see
  [Configuring credentials](#configuring-credentials). Defaults to `rcloneui` /
  `Rcl0ne-Ui-Local!`.
- **Remotes**: [generate-rclone-config.ps1](generate-rclone-config.ps1) builds an rclone config file
  with one remote per account in `config/storage-config.json` - `iiswmshotstore` and `iiswmscoldstore`
  by default, named after the account itself - so both tiers are already set up once you log in, no
  manual "add a remote" step needed. Re-run `setup-podman-emulators.bat` after editing
  `storage-config.json` (a new account, a rotated key) to regenerate this and pick up the change - the
  rclone-ui container is always removed and recreated fresh, same as every other container here.
- **Blob only** - like the read-only listing below, rclone's `azureblob` backend has no Table Storage
  support here either. Use Azure Storage Explorer (below) for Table Storage.
- If the page won't load: check `podman logs iis-wms-rclone-ui` - most likely either Azurite itself
  isn't up yet (see "Azurite is ready" in the console output above), or the container is still
  downloading its GUI assets from GitHub on first start (needs outbound internet once).

#### Browsing storage accounts in a web browser (read-only, no login)

For a quick read-only view with nothing to log into - or as a fallback if the rclone web UI above
isn't reachable (e.g. no outbound internet for its first-run GUI download) - run
[scripts/generate-local-stack-summary.ps1](../generate-local-stack-summary.ps1) and open its
"Browse containers/blobs" section instead. This exists because Azurite's Blob/Table REST calls need a
signed request, so there's no plain URL to open against Azurite directly:

- **It lists containers/blobs itself, at generation time** - the script signs a short-lived,
  read+list [Account SAS](https://learn.microsoft.com/rest/api/storageservices/create-account-sas)
  per account with .NET's own `HMACSHA256` (the same "hand-sign the REST call instead of adding a
  new SDK dependency" approach `setup-cosmos-data.ps1` already uses for Cosmos DB, per CLAUDE.md's
  "never introduce a new package without approval" rule), calls Azurite's List Containers/List Blobs
  REST API directly, and renders the results as plain `<details>` elements - no client-side
  JavaScript/CORS involved at all. Predates the rclone web UI above (see that section's own note on
  why `ghcr.io/adrianhall/azurite-ui`, a more purpose-built option, wasn't used instead) and is kept
  as a no-login fallback, not because this one's static-listing approach is preferred.
- **Not a live view** - it's a snapshot as of when you ran the script, same as the rest of that
  page. Re-run it to refresh the listing (and the SAS token, valid 30 days).
- **Blob only** - it has no Table Storage support. Use Azure Storage Explorer (below) for Table
  Storage, or for a live/writable view.
- Each blob has a direct download link (the same SAS token, scoped to that blob) - clicking it is a
  plain navigation, not a `fetch`, so it isn't subject to CORS either.
- If every account shows "Azurite isn't reachable right now": Azurite itself isn't up - check
  `podman logs iis-wms-storage-emulator`, then re-run the summary script once it is.

## The Azure CLI tools container

[image/tools.Containerfile](image/tools.Containerfile) builds a minimal image from
`mcr.microsoft.com/azure-cli:2.88.0-azurelinux3.0` (Microsoft's currently maintained base -
the older Alpine-based tag stopped receiving updates after Azure CLI 2.63.0, August 2024). The
script starts it as `iis-wms-emulators-tools`, running `sleep infinity` so it stays up for you to
exec into:

```bat
podman exec -it iis-wms-emulators-tools az --version
podman exec -it iis-wms-emulators-tools az login
```

**What this container is not**: a way to provision the Cosmos DB/Service Bus emulators above.
`az cosmosdb`/`az servicebus` subcommands talk to real Azure Resource Manager - neither emulator
runs an ARM control plane locally, so there's nothing for those commands to reach here. The Cosmos
DB Emulator gets its database/containers/seed data from `setup-cosmos-data.ps1` (see "Database,
containers, and seed data" above), not from `az` or the app's own `CosmosClient` (which per
cosmos-db.instructions.md §2 never creates databases/containers itself); the Service Bus emulator
gets its queues from `config/service-bus-config.json` above, applied when its container starts.
**Azurite is the one exception** - `az storage container/table create` (via
`setup-storage-data.ps1`) genuinely does provision it, since that's a data-plane call straight to
Azurite's own storage endpoint, not ARM - see "Azurite (Blob Storage + Table Storage) specifics"
above. Beyond that scripted use, this container also exists for genuinely ad-hoc uses - e.g., logging
into a real Azure subscription from the same Podman network to compare a setting, or just having
`az`/`curl` available without installing them on your host.

## Tear down

```bat
podman rm -f iis-wms-cosmos-emulator iis-wms-servicebus-emulator iis-wms-emulators-mssql iis-wms-storage-emulator iis-wms-emulators-tools iis-wms-rclone-ui
podman network rm iis-wms-emulators-net
```

None of these containers use a named Podman volume (see "Quick start" above), so there's no
separate volume-cleanup step the way `scripts\local-kafka`'s `iis-wms-kafka-data` volume needs one.

## Troubleshooting

- **Service Bus emulator never becomes ready / `podman logs iis-wms-servicebus-emulator` shows it
  waiting on SQL**: SQL Server Linux can take longer than the emulator's own `SQL_WAIT_INTERVAL`
  (set to 15 seconds here) to finish starting on a slow machine - re-run the script; the SQL Server
  Linux container itself is left running from the failed attempt, so the retry should be faster.
- **Cosmos DB Emulator health probe never returns ready**: check `podman logs
  iis-wms-cosmos-emulator` - on first pull this image can take a while to initialize; the script
  polls for 2 minutes (60 retries × 2 seconds) before giving up.
- **A .NET client throws a TLS/certificate trust error against the Cosmos DB Emulator**: expected
  the first time on a fresh machine per Microsoft's own Java SDK certificate-trust notes on
  [emulator-linux](https://learn.microsoft.com/azure/cosmos-db/emulator-linux#installing-certificates-for-java-sdk) -
  the .NET SDK's own certificate handling for this emulator isn't covered by this script; consult
  that page's guidance if this bites you.
- **"Provisioning Blob containers/tables" step fails**: check `podman logs iis-wms-storage-emulator`
  first (Azurite itself may not actually be ready yet, despite the wait loop above passing - a
  transient 5xx from Azurite's own startup), then
  `podman exec -it iis-wms-emulators-tools az storage container list --connection-string "..."`
  (same connection string `setup-storage-data.ps1` builds for that account - see its header comment)
  to reproduce the failing call directly.
- **`az storage` (or the app itself) reports "Invalid storage account" / `AccountNotFound` /
  authentication failures against Azurite**: almost always means the account name in the connection
  string doesn't match one of the names in `config/storage-config.json`'s `accounts` array -
  remember `devstoreaccount1` itself is disabled once any custom account is configured (see "Multiple
  accounts" above). Check `podman logs iis-wms-storage-emulator` for the account list Azurite actually
  loaded at startup, and compare against what `get-azurite-accounts-env.ps1` would print for the
  current `config/storage-config.json` (run it directly: `powershell -NoProfile -ExecutionPolicy
  Bypass -File scripts\local-emulators\get-azurite-accounts-env.ps1`).
- **The generated summary page's "Browse containers/blobs" section shows an account as
  unreachable**: see "Browsing storage accounts in a web browser (read-only, no login)" under
  "Azurite (Blob Storage + Table Storage) specifics" above.
- **rclone web UI (`http://localhost:5572`) won't load, or shows no remotes**: check `podman logs
  iis-wms-rclone-ui` - a blank/slow first load usually means it's still downloading its GUI assets
  from GitHub (needs outbound internet once per image; re-check after a minute). If it loads but a
  remote fails to browse, confirm Azurite itself is up (`podman logs iis-wms-storage-emulator`) and
  that `config/storage-config.json` still declares that account - `generate-rclone-config.ps1` only
  builds remotes from whatever's in that file as of the last `setup-podman-emulators.bat` run.
