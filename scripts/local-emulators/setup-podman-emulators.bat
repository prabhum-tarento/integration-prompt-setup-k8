@echo off
REM Starts the Azure Cosmos DB Linux Emulator (vNext), the Azure Service Bus emulator (plus its
REM required SQL Server Linux dependency), and Azurite (Blob Storage + Table Storage) under Podman,
REM matching this app's local-dev configuration shape - CosmosDb:AccountEndpoint/EmulatorKey,
REM ServiceBus:ConnectionString/QueueName/BulkInventoryImport:QueueName, and
REM BlobStorage:Hot/Cold:AccountUri in
REM src/Api/IIS.WMS.Consumer.Api/appsettings.Development.json (see cosmos-db.instructions.md Sec1 and
REM integration-resiliency.instructions.md Sec2/Sec5/Sec9) - not the real Confluent-Cloud-shaped Kafka
REM credentials scripts/local-kafka covers, which is why this is a separate folder/network rather than
REM folded into that one.
REM
REM Also builds and starts a fourth, general-purpose "tools" container (image/tools.Containerfile)
REM with the Azure CLI installed, on the same Podman network - see that Containerfile's header
REM comment for exactly what it is (and isn't) useful for here: it does NOT provision the Cosmos
REM DB/Service Bus emulators below (az cosmosdb/az servicebus talk to real Azure Resource Manager,
REM not either emulator's local data plane) - it's just an ad-hoc admin shell with `az`/curl
REM available from inside the same network namespace. It IS, however, used by
REM setup-storage-data.ps1 (below) to provision Azurite's Blob containers/tables, since `az storage`
REM is a data-plane command straight to the storage endpoint, not an ARM call - see that script's own
REM header comment and README.md's "What this container is not" for the distinction.
REM
REM Usage: setup-podman-emulators.bat -AcceptEula
REM
REM The Service Bus emulator and its SQL Server Linux dependency each require accepting their own
REM Microsoft license terms before they'll run (ACCEPT_EULA=Y baked into both containers below) -
REM this script will NOT set that silently. Pass -AcceptEula (or set ACCEPT_EULA=Y in your
REM environment first) once you've read and agree to both:
REM   Service Bus emulator EULA: https://github.com/Azure/azure-service-bus-emulator-installer/blob/main/EMULATOR_EULA.txt
REM   SQL Server Linux EULA:     https://go.microsoft.com/fwlink/?LinkId=746388
REM Without it, this script prints those links and exits without starting anything.
REM
REM Every container here is removed and recreated fresh on every run (same reasoning as
REM setup-podman-kafka.bat: guarantees the current config/Config.json is always picked up instead
REM of silently continuing on a stale one) - none of the four emulators need data to persist
REM across runs for local manual testing the way the Kafka broker's topic/offset history does, so
REM unlike that script there's no named volume here. If you need seeded Cosmos DB data to survive a
REM re-run, see the Cosmos DB Emulator's own ENABLE_INIT_DATA/-v .../data option
REM (docs/ai's cosmos-db.instructions.md doesn't cover this - see
REM https://learn.microsoft.com/azure/cosmos-db/emulator-linux "Persist data across restarts").

REM Plain setlocal, not enabledelayedexpansion - nothing here uses !var! delayed-expansion syntax,
REM and turning it on silently eats the literal "!" in MSSQL_SA_PASSWORD/RCLONE_UI_PASSWORD's
REM default values (and any override from *-config.json) every time they're expanded via %VAR% -
REM an unmatched "!" on a line gets dropped when delayed expansion is active. That's what broke the
REM rclone web UI login: the password rclone actually enforced ended up missing its trailing "!"
REM while storage-config.json still had it.
setlocal

REM vnext-latest/latest are Microsoft's own current documented tags for these two emulator images
REM (learn.microsoft.com/azure/cosmos-db/emulator-linux and .../service-bus-messaging/test-locally-
REM with-service-bus-emulator, checked 2026-07) - neither publishes a numbered release tag to pin
REM to instead, unlike mcr.microsoft.com/mssql/server (2022-latest, at least version-major-pinned)
REM and the Azure CLI tools image below (fully pinned). Flagging this as a deliberate exception to
REM this repo's usual "pin exact versions" convention, not an oversight.
set COSMOS_IMAGE=mcr.microsoft.com/cosmosdb/linux/azure-cosmos-emulator:vnext-latest
set SERVICEBUS_IMAGE=mcr.microsoft.com/azure-messaging/servicebus-emulator:latest
set MSSQL_IMAGE=mcr.microsoft.com/mssql/server:2022-latest
REM Unlike the two emulators above, Azurite does publish numbered release tags on mcr.microsoft.com
REM (github.com/Azure/Azurite/blob/main/README.mcr.md - 3.36.0 dated 2025-11-05 at the time this was
REM written) - pinned per this repo's usual convention instead of ":latest".
set AZURITE_IMAGE=mcr.microsoft.com/azure-storage/azurite:3.36.0
set TOOLS_IMAGE=iis-wms-emulators-tools
REM rclone (docker.io/rclone/rclone) - a real interactive web UI (browse/upload/download/delete/rename)
REM for Azurite's Blob accounts, since Azurite itself has none - see "rclone web UI (Blob browser)"
REM below and local-emulators\README.md's "Interactive web UI (rclone)" section. Pinned per this
REM repo's usual convention - Docker Hub publishes numbered release tags for this image, not just
REM "latest" (unlike the two vnext-latest/latest exceptions above).
set RCLONE_IMAGE=docker.io/rclone/rclone:1.74.4
set NETWORK_NAME=iis-wms-emulators-net
set COSMOS_CONTAINER=iis-wms-cosmos-emulator
set MSSQL_CONTAINER=iis-wms-emulators-mssql
set SERVICEBUS_CONTAINER=iis-wms-servicebus-emulator
set AZURITE_CONTAINER=iis-wms-storage-emulator
set TOOLS_CONTAINER=iis-wms-emulators-tools
set RCLONE_UI_CONTAINER=iis-wms-rclone-ui
set CONFIG_DIR=%~dp0config
set IMAGE_DIR=%~dp0image

REM --- Host ports (..\ports.env, shared with scripts\local-kafka\setup-podman-kafka.bat) ---
REM Defaults set first so a missing file, or one missing a key, still leaves every port usable -
REM ports.env only needs to declare the keys you want to override. See that file's own header
REM comment and README.md's "Configuring ports" section for what each port is used for and why
REM changing one here does not also update appsettings.Development.json/user-secrets.
set COSMOS_ENDPOINT_PORT=8081
set COSMOS_HEALTH_PORT=8080
set COSMOS_EXPLORER_PORT=1234
set SERVICEBUS_AMQP_PORT=5672
set SERVICEBUS_MGMT_PORT=5300
set AZURITE_BLOB_PORT=10000
set AZURITE_TABLE_PORT=10002
set RCLONE_UI_PORT=5572
set PORTS_FILE=%~dp0..\ports.env
if exist "%PORTS_FILE%" (
    for /f "usebackq eol=# tokens=1,2 delims==" %%A in ("%PORTS_FILE%") do set "%%A=%%B"
) else (
    echo %PORTS_FILE% not found - using built-in default ports.
)

REM Local-only credentials (config/service-bus-config.json's "Credentials" key) - the SQL Server
REM Linux SA password and the Service Bus emulator's own SharedAccessKeyName/SharedAccessKey.
REM Piggybacks on the same file already mounted into the Service Bus emulator container below
REM (rather than a separate credentials file) since both only ever matter to that
REM container/its SQL dependency - "Credentials" is a sibling of "UserConfig" the emulator
REM itself doesn't recognize, not read into it, so it's ignored by the emulator's own config
REM loader (unknown top-level properties don't fail its parse - if a future emulator image ever
REM starts rejecting unknown properties, check "podman logs iis-wms-servicebus-emulator" first
REM and move this back out to its own file). None of these ever talk to a real Azure SQL/Service
REM Bus resource, so (like the Kafka script's SASL credentials) they don't need to come from
REM user-secrets/Key Vault. Defaults set first so a missing file, or one missing a key, still
REM leaves every credential usable; edit config/service-bus-config.json's "Credentials" key (then
REM re-run this script) for different local values instead of editing these defaults directly.
REM MSSQL_SA_PASSWORD must meet SQL Server's own complexity policy (8+ chars, upper/lower/digit/
REM symbol) if you change it. SERVICEBUS_SAS_KEY_NAME/SERVICEBUS_SAS_KEY are pure display values,
REM not enforced by the emulator at all - UseDevelopmentEmulator=true bypasses real SAS signature
REM checks (see README.md's connection-string note) - changing them only changes what this
REM script's own console output prints, not any actual authorization.
set MSSQL_SA_PASSWORD=L0cal-Sb-Emul4tor!
set SERVICEBUS_SAS_KEY_NAME=RootManageSharedAccessKey
set SERVICEBUS_SAS_KEY=SAS_KEY_VALUE
for /f "usebackq delims=" %%A in (`powershell -NoProfile -ExecutionPolicy Bypass -Command "$c = Get-Content -Raw '%CONFIG_DIR%\service-bus-config.json' | ConvertFrom-Json; if ($c.Credentials.Sql.SaPassword) { $c.Credentials.Sql.SaPassword }"`) do set MSSQL_SA_PASSWORD=%%A
for /f "usebackq delims=" %%A in (`powershell -NoProfile -ExecutionPolicy Bypass -Command "$c = Get-Content -Raw '%CONFIG_DIR%\service-bus-config.json' | ConvertFrom-Json; if ($c.Credentials.ServiceBus.SharedAccessKeyName) { $c.Credentials.ServiceBus.SharedAccessKeyName }"`) do set SERVICEBUS_SAS_KEY_NAME=%%A
for /f "usebackq delims=" %%A in (`powershell -NoProfile -ExecutionPolicy Bypass -Command "$c = Get-Content -Raw '%CONFIG_DIR%\service-bus-config.json' | ConvertFrom-Json; if ($c.Credentials.ServiceBus.SharedAccessKey) { $c.Credentials.ServiceBus.SharedAccessKey }"`) do set SERVICEBUS_SAS_KEY=%%A

REM Cosmos DB Emulator master key - read from config/cosmos-db-config.json's emulatorKey (same file
REM setup-cosmos-data.ps1 already reads databaseName/partitionKey/containers from - see that script's
REM own header comment for the full precedence rule) rather than hardcoded here, so there's exactly
REM one place to change it. Falls back to Microsoft's well-known, publicly documented emulator key -
REM the same fixed value on every machine running this image, not a secret specific to this install -
REM if the config file is missing the field entirely. Still read into user-secrets as
REM CosmosDb:EmulatorKey (cosmos-db.instructions.md Sec1), never appsettings.json, for consistency with
REM how every other local credential in this repo is handled - not because this particular value needs
REM protecting.
set COSMOS_EMULATOR_KEY=C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==
for /f "usebackq delims=" %%A in (`powershell -NoProfile -ExecutionPolicy Bypass -Command "$k = (Get-Content -Raw '%CONFIG_DIR%\cosmos-db-config.json' | ConvertFrom-Json).emulatorKey; if ($k) { $k }"`) do set COSMOS_EMULATOR_KEY=%%A

REM rclone web UI's own RC-endpoint login (config/storage-config.json's "rcloneUi" key, a sibling of
REM "accounts" - not itself an Azurite/Azure credential, just this one container's HTTP basic auth) -
REM see "rclone web UI (Blob browser)" further below. Falls back to these defaults (same "local,
REM throwaway value" reasoning as every other credential in this script) if the key/file is missing.
set RCLONE_UI_USERNAME=rcloneui
set RCLONE_UI_PASSWORD=Rcl0ne-Ui-Local!
for /f "usebackq delims=" %%A in (`powershell -NoProfile -ExecutionPolicy Bypass -Command "$c = Get-Content -Raw '%CONFIG_DIR%\storage-config.json' | ConvertFrom-Json; if ($c.rcloneUi.username) { $c.rcloneUi.username }"`) do set RCLONE_UI_USERNAME=%%A
for /f "usebackq delims=" %%A in (`powershell -NoProfile -ExecutionPolicy Bypass -Command "$c = Get-Content -Raw '%CONFIG_DIR%\storage-config.json' | ConvertFrom-Json; if ($c.rcloneUi.password) { $c.rcloneUi.password }"`) do set RCLONE_UI_PASSWORD=%%A

REM --- EULA gate (Service Bus emulator + its SQL Server Linux dependency only - the Cosmos DB
REM Emulator/Azure CLI images below have no equivalent runtime env-var gate) ---
set ACCEPT_EULA_FLAG=0
if /i "%~1"=="-AcceptEula" set ACCEPT_EULA_FLAG=1
if /i "%ACCEPT_EULA%"=="Y" set ACCEPT_EULA_FLAG=1

if %ACCEPT_EULA_FLAG% neq 1 (
    echo(
    echo This script starts the Azure Service Bus emulator and its SQL Server Linux dependency,
    echo both of which require accepting their own license terms before they'll run:
    echo(
    echo   Service Bus emulator EULA: https://github.com/Azure/azure-service-bus-emulator-installer/blob/main/EMULATOR_EULA.txt
    echo   SQL Server Linux EULA:     https://go.microsoft.com/fwlink/?LinkId=746388
    echo(
    echo Re-run as "setup-podman-emulators.bat -AcceptEula" once you've read and agree to both -
    echo or set ACCEPT_EULA=Y in your environment first. Nothing has been started.
    exit /b 1
)

echo(
echo === Checking Podman ===
podman version >nul 2>&1
if errorlevel 1 (
    echo Podman is not available on PATH - install/start Podman Desktop first.
    exit /b 1
)

echo(
echo === Network ===
podman network inspect %NETWORK_NAME% >nul 2>&1
if errorlevel 1 (
    podman network create %NETWORK_NAME% || exit /b 1
) else (
    echo %NETWORK_NAME% already exists.
)

echo(
echo === Cosmos DB Emulator ===
REM --protocol https is required, not optional - the .NET SDK (Microsoft.Azure.Cosmos, used
REM throughout this repo per cosmos-db.instructions.md) doesn't support talking to the emulator
REM over plain HTTP, unlike this image's own http default (learn.microsoft.com/azure/cosmos-db/
REM emulator-linux "HTTPS mode"). Port 8081 is the Cosmos endpoint itself (matches
REM CosmosDb:AccountEndpoint=https://localhost:8081 in appsettings.Development.json); 8080 is the
REM emulator's own health-probe endpoint (/alive, /ready, /status - used by the wait loop below,
REM not by the app); 1234 is the bundled Data Explorer UI. All three are the container's internal,
REM fixed ports - only the host-side mapping is configurable, via config/ports.env.
podman rm -f %COSMOS_CONTAINER% >nul 2>&1
podman run -d --name %COSMOS_CONTAINER% --network %NETWORK_NAME% ^
    -p %COSMOS_ENDPOINT_PORT%:8081 -p %COSMOS_HEALTH_PORT%:8080 -p %COSMOS_EXPLORER_PORT%:1234 ^
    %COSMOS_IMAGE% --protocol https || exit /b 1

echo(
echo === Waiting for Cosmos DB Emulator to become ready ===
set /a RETRIES=60
:wait_cosmos
curl -sf http://localhost:%COSMOS_HEALTH_PORT%/ready >nul 2>&1
if not errorlevel 1 goto cosmos_ready
set /a RETRIES-=1
if %RETRIES% leq 0 (
    echo Cosmos DB Emulator did not become ready in time - check "podman logs %COSMOS_CONTAINER%".
    exit /b 1
)
timeout /t 2 >nul
goto wait_cosmos
:cosmos_ready
echo Cosmos DB Emulator is ready.

echo(
echo === Provisioning Cosmos DB database/containers and seed data ===
REM The Cosmos DB Emulator container above is always removed and recreated fresh (see header
REM comment), so its database/containers never survive a re-run either - reprovision and reseed
REM them every time via setup-cosmos-data.ps1, which talks to the emulator's REST API directly
REM (no new SDK/tool dependency) and reads container-named JSON files from .\data.
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0setup-cosmos-data.ps1" ^
    -Endpoint "https://localhost:%COSMOS_ENDPOINT_PORT%" -Key "%COSMOS_EMULATOR_KEY%" ^
    -DataExplorerUrl "http://localhost:%COSMOS_EXPLORER_PORT%" || exit /b 1

echo(
echo === SQL Server Linux (Service Bus emulator's metadata store) ===
REM Always removed and recreated fresh, same reasoning as every container in setup-podman-kafka.bat
REM - this holds no data worth preserving across runs (it's only the Service Bus emulator's own
REM internal metadata store, reseeded from config/service-bus-config.json below on every start).
podman rm -f %MSSQL_CONTAINER% >nul 2>&1
podman run -d --name %MSSQL_CONTAINER% --network %NETWORK_NAME% ^
    -e ACCEPT_EULA=Y ^
    -e MSSQL_SA_PASSWORD=%MSSQL_SA_PASSWORD% ^
    %MSSQL_IMAGE% || exit /b 1

echo(
echo === Azure Service Bus emulator ===
REM SQL_WAIT_INTERVAL gives the emulator time to find SQL Server Linux ready on its own, rather than
REM this script polling SQL Server directly first - matches the official docker-compose.yaml
REM (learn.microsoft.com/azure/service-bus-messaging/test-locally-with-service-bus-emulator).
REM Port 5672 is the AMQP endpoint ServiceBusClient connects to; 5300 is the emulator's own
REM management/health-check HTTP API (used by the wait loop below and by the Service Bus
REM Administration Client - see README.md's connection-string note on why that one needs the port
REM appended explicitly). Both are the container's internal, fixed ports (EMULATOR_HTTP_PORT below
REM sets the internal management port to match) - only the host-side mapping is configurable, via
REM config/ports.env. config/service-bus-config.json declares this app's four queues
REM (inventory-events, inventory-state-changed, inventory-adjusted - each RequiresSession: true,
REM matching the SessionId={WarehouseId}:{Sku} the Kafka relay sets per
REM integration-resiliency.instructions.md Sec1/Sec2 - and inventory-bulk-import, non-session, matching
REM the separate non-session bulk-import consumer).
podman rm -f %SERVICEBUS_CONTAINER% >nul 2>&1
podman run -d --name %SERVICEBUS_CONTAINER% --network %NETWORK_NAME% ^
    -p %SERVICEBUS_AMQP_PORT%:5672 -p %SERVICEBUS_MGMT_PORT%:5300 ^
    -v "%CONFIG_DIR%\service-bus-config.json:/ServiceBus_Emulator/ConfigFiles/Config.json:Z" ^
    -e SQL_SERVER=%MSSQL_CONTAINER% ^
    -e MSSQL_SA_PASSWORD=%MSSQL_SA_PASSWORD% ^
    -e ACCEPT_EULA=Y ^
    -e EMULATOR_HTTP_PORT=5300 ^
    -e SQL_WAIT_INTERVAL=15 ^
    %SERVICEBUS_IMAGE% || exit /b 1

echo(
echo === Waiting for Service Bus emulator to become ready ===
set /a RETRIES=60
:wait_servicebus
curl -sf http://localhost:%SERVICEBUS_MGMT_PORT%/health >nul 2>&1
if not errorlevel 1 goto servicebus_ready
set /a RETRIES-=1
if %RETRIES% leq 0 (
    echo Service Bus emulator did not become ready in time - check "podman logs %SERVICEBUS_CONTAINER%"
    echo and "podman logs %MSSQL_CONTAINER%".
    exit /b 1
)
timeout /t 2 >nul
goto wait_servicebus
:servicebus_ready
echo Service Bus emulator is ready.

echo(
echo === Azurite account list (config/storage-config.json) ===
REM Built before starting the Azurite container below (not after), since AZURITE_ACCOUNTS is only
REM read at container start - see get-azurite-accounts-env.ps1's own header comment for the full
REM reasoning (including why setting this disables Azurite's own default "devstoreaccount1" account).
REM A separate tiny script rather than inline PowerShell here so its JSON-parsing/validation logic
REM isn't duplicated between this .bat and setup-storage-data.ps1 (which provisions each account's
REM containers/tables once Azurite and the tools container are both up, further below).
for /f "delims=" %%A in ('powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0get-azurite-accounts-env.ps1" -ConfigDir "%CONFIG_DIR%"') do set AZURITE_ACCOUNTS_ENV=%%A
if not defined AZURITE_ACCOUNTS_ENV (
    echo Failed to read account list from config\storage-config.json - see the error above.
    exit /b 1
)

echo(
echo === Azurite (Blob Storage + Table Storage) ===
REM Only the two services this app actually needs are published/documented - Queue Storage (Azurite's
REM third service, container-internal port 10001) runs inside the container regardless (the "azurite"
REM binary always starts all three; there's no combined "blob+table only" binary, only per-service
REM ones - azurite-blob/azurite-queue/azurite-table) but its port is neither published nor used here.
REM --blobHost/--tableHost 0.0.0.0 (not the image's own 127.0.0.1 default) are required for Podman's
REM port publishing to actually reach the listener from outside the container's network namespace -
REM see github.com/Azure/Azurite/blob/main/README.mcr.md. --skipApiVersionCheck avoids a hard
REM rejection if this app's Azure.Storage.Blobs package version (Directory.Packages.props) ever
REM drifts ahead of what this pinned Azurite version itself recognizes - a soft warning instead of a
REM blocked request, same rationale Microsoft's own docs give this flag. AZURITE_ACCOUNTS (computed
REM above) is what actually creates the Hot/Cold accounts declared in config/storage-config.json -
REM see that env var's own Azurite documentation
REM (github.com/Azure/Azurite/blob/main/README.md#customizable-storage-accounts--keys). No named
REM volume/persistence, no EULA gate - same reasoning as the other two emulators above (Azurite is
REM MIT-licensed; nothing to accept).
podman rm -f %AZURITE_CONTAINER% >nul 2>&1
podman run -d --name %AZURITE_CONTAINER% --network %NETWORK_NAME% ^
    -p %AZURITE_BLOB_PORT%:10000 -p %AZURITE_TABLE_PORT%:10002 ^
    -e AZURITE_ACCOUNTS=%AZURITE_ACCOUNTS_ENV% ^
    %AZURITE_IMAGE% azurite --blobHost 0.0.0.0 --tableHost 0.0.0.0 --skipApiVersionCheck || exit /b 1

echo(
echo === Waiting for Azurite to become ready ===
REM Azurite has no dedicated health-probe endpoint like the two emulators above - any HTTP response at
REM all (even Azurite's own 400/401 for an unauthenticated/malformed request) proves the listener is up
REM and initialized, so this deliberately omits curl's -f (which would treat that same response as a
REM failure).
set /a RETRIES=60
:wait_azurite
curl -s http://localhost:%AZURITE_BLOB_PORT%/ >nul 2>&1 && curl -s http://localhost:%AZURITE_TABLE_PORT%/ >nul 2>&1
if not errorlevel 1 goto azurite_ready
set /a RETRIES-=1
if %RETRIES% leq 0 (
    echo Azurite did not become ready in time - check "podman logs %AZURITE_CONTAINER%".
    exit /b 1
)
timeout /t 2 >nul
goto wait_azurite
:azurite_ready
echo Azurite is ready.

echo(
echo === Tools image (Azure CLI) ===
REM See image/tools.Containerfile's header comment for what this container is - and isn't - for.
REM Runs "sleep infinity" as its foreground process rather than exiting immediately, purely so it
REM stays around for you to "podman exec -it %TOOLS_CONTAINER% az ..." into on demand; it isn't
REM itself a long-running service the app or the emulators above depend on.
podman build -t %TOOLS_IMAGE% -f "%IMAGE_DIR%\tools.Containerfile" "%IMAGE_DIR%" || exit /b 1
podman rm -f %TOOLS_CONTAINER% >nul 2>&1
podman run -d --name %TOOLS_CONTAINER% --network %NETWORK_NAME% %TOOLS_IMAGE% sleep infinity || exit /b 1

echo(
echo === Provisioning Blob containers/tables (per account) ===
REM Azurite's containers/tables don't survive its own container being recreated fresh above either -
REM reprovision every time via setup-storage-data.ps1, which execs `az storage container/table create`
REM inside %TOOLS_CONTAINER% (just started) rather than adding a new SDK dependency to this script -
REM see that script's own header comment for why. Loops over every account in
REM .\config\storage-config.json (the same role cosmos-db-config.json plays for the Cosmos DB
REM Emulator), and prints each account's localhost connection string at the end - that output is this
REM script's source of truth for what to put in appsettings.Development.json/user-secrets, not the
REM lines further below (which only cover Cosmos DB/Service Bus).
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0setup-storage-data.ps1" ^
    -ToolsContainer "%TOOLS_CONTAINER%" -AzuriteContainer "%AZURITE_CONTAINER%" ^
    -AzuriteBlobPort "%AZURITE_BLOB_PORT%" -AzuriteTablePort "%AZURITE_TABLE_PORT%" || exit /b 1

echo(
echo === rclone web UI (Blob browser) ===
REM A real interactive web UI for Azurite's Blob accounts (browse/upload/download/delete/rename) -
REM Azurite itself has none, and the more purpose-built ghcr.io/adrianhall/azurite-ui turned out not
REM to be publicly pullable (see generate-local-stack-summary.ps1's own header comment on its
REM SAS-listing fallback, which stays in place regardless of this container - a quick read-only view
REM with no login needed). rclone (https://rclone.org/docker/) is a well-known, actively maintained
REM project whose azureblob backend documents Azurite emulator support directly - `rclone rcd
REM --rc-web-gui` serves that same backend as a browser UI. See local-emulators\README.md's
REM "Interactive web UI (rclone)" section for the full walkthrough.
set RCLONE_CONF=%TEMP%\iis-wms-rclone.conf
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0generate-rclone-config.ps1" ^
    -OutFile "%RCLONE_CONF%" -AzuriteContainer "%AZURITE_CONTAINER%" || exit /b 1
podman rm -f %RCLONE_UI_CONTAINER% >nul 2>&1
REM --rc-addr :5572 (not the image's own localhost-only default) is required for Podman's port
REM publishing to reach the listener, same "0.0.0.0, not 127.0.0.1" reasoning as Azurite's own
REM --blobHost/--tableHost flags above. --rc-web-gui-no-open-browser skips trying to launch a
REM desktop browser from inside the container (there isn't one). --rc-user/--rc-pass gate the RC
REM endpoint the web GUI itself talks to - without them it would serve with no login at all.
podman run -d --name %RCLONE_UI_CONTAINER% --network %NETWORK_NAME% ^
    -p %RCLONE_UI_PORT%:5572 ^
    -v "%RCLONE_CONF%:/config/rclone/rclone.conf:ro,Z" ^
    %RCLONE_IMAGE% rcd --rc-web-gui --rc-web-gui-no-open-browser --rc-web-gui-update ^
    --rc-addr :5572 --rc-user %RCLONE_UI_USERNAME% --rc-pass %RCLONE_UI_PASSWORD% || exit /b 1

REM sb://localhost with no port implicitly means the emulator's default AMQP port (5672) - only
REM spelled out explicitly below when config/ports.env has remapped it away from that default.
set SB_AMQP_ENDPOINT=sb://localhost
if not "%SERVICEBUS_AMQP_PORT%"=="5672" set SB_AMQP_ENDPOINT=sb://localhost:%SERVICEBUS_AMQP_PORT%

echo(
echo === Local configuration ===
echo (Ports below come from config/ports.env - see README.md's "Configuring ports" section)
echo CosmosDb:AccountEndpoint = https://localhost:%COSMOS_ENDPOINT_PORT%
echo CosmosDb:EmulatorKey     = %COSMOS_EMULATOR_KEY%   (put this in user-secrets, never appsettings.json - cosmos-db.instructions.md Sec1)
echo Cosmos DB Data Explorer  = http://localhost:%COSMOS_EXPLORER_PORT%
echo Cosmos DB database/containers (InventoryDb: InventoryEvents, OrderArchive, BulkInventoryImports) are provisioned and seeded from .\data\*.json - see setup-cosmos-data.ps1
echo(
echo ServiceBus:ConnectionString = Endpoint=%SB_AMQP_ENDPOINT%;SharedAccessKeyName=%SERVICEBUS_SAS_KEY_NAME%;SharedAccessKey=%SERVICEBUS_SAS_KEY%;UseDevelopmentEmulator=true;
echo ServiceBus:QueueName                        = inventory-events
echo ServiceBus:BulkInventoryImport:QueueName     = inventory-bulk-import
echo (Service Bus Administration Client operations need the port appended: .../localhost:%SERVICEBUS_MGMT_PORT%;...)
echo(
echo BlobStorage:Hot/Cold:AccountUri - see the per-account connection strings setup-storage-data.ps1
echo just printed above (one real Azurite account per tier, matching integration-resiliency.instructions.md
echo Sec5 - not a single shared "UseDevelopmentStorage=true" account). Accounts/containers/tables are
echo declared in config\storage-config.json - add an account (or a container/table to an existing one)
echo there and re-run this script; no other wiring is needed.
echo(
echo rclone web UI (Blob browser) = http://localhost:%RCLONE_UI_PORT%/
echo   Username: %RCLONE_UI_USERNAME%
echo   Password: %RCLONE_UI_PASSWORD%
echo   (change these via config/storage-config.json's "rcloneUi" key - see README.md's
echo   "Configuring credentials" section. Every account above is already configured as its own
echo   remote, named after the account, once you're logged in.)
echo(
echo Tools container (Azure CLI): podman exec -it %TOOLS_CONTAINER% az --version
echo(
echo Tear down: podman rm -f %COSMOS_CONTAINER% %SERVICEBUS_CONTAINER% %MSSQL_CONTAINER% %AZURITE_CONTAINER% %TOOLS_CONTAINER% %RCLONE_UI_CONTAINER% ^&^& podman network rm %NETWORK_NAME%

endlocal
