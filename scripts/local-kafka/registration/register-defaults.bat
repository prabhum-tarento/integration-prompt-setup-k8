@echo off
REM Registers this app's default topics, Avro schemas, and consumer groups against an
REM already-running local Kafka + Schema Registry stack, so Kafka UI (http://localhost:8090)
REM shows them immediately instead of only after the real app runs and consumes for the
REM first time. Also publishes each event's sample message(s) (Avro-encoded, via Kafka REST
REM Proxy) and consumes them into that topic's consumer groups, so those groups show up
REM caught up rather than lagging - generalizing what setup-podman-kafka.bat used to do only
REM for one hardcoded InventoryStateChanged sample (see push-inventory-state-changed.ps1).
REM Works against any of the three setups in this folder - pass the Kafka container's actual
REM name if it isn't the .bat/image path's default.
REM
REM Everything under registration\events\ is discovered dynamically - nothing here is
REM hardcoded to a specific topic/event/group name:
REM   events\<topic>\                    - one folder per Kafka topic (folder name = topic name)
REM   events\<topic>\*.json              - a direct JSON file (not a folder) holding a JSON
REM                                        array of consumer group names to pre-create
REM                                        against that topic. If a topic folder has no such
REM                                        file, "$Default" is registered as its sole
REM                                        consumer group instead.
REM   events\<topic>\<event-name>\       - one folder per Avro-schema'd event carried on that topic
REM   events\<topic>\<event-name>\*.avsc - the event's Avro schema, registered with Schema Registry
REM   events\<topic>\<event-name>\*.json - sample event message(s), each shaped
REM                                        { "headers": {...}, "body": {...} } - published via
REM                                        Kafka REST Proxy (see publish-event-sample.ps1) and
REM                                        consumed into every consumer group registered for
REM                                        that topic, skipped with a warning (not a failure)
REM                                        if Kafka REST Proxy isn't reachable - some setups in
REM                                        this folder (e.g. the custom-image path) don't run it
REM
REM Registration runs in four strict phases, each covering every topic before the next phase
REM starts, rather than finishing one topic end-to-end before moving to the next:
REM   1. Create every topic.
REM   2. Register every consumer group (needs its topic to already exist).
REM   3. Register every schema (independent of topics/groups, but schemas are registered
REM      before phase 4 needs them to resolve a schema_id).
REM   4. Publish each event's sample message(s) and consume them into every consumer group
REM      registered for that topic (needs every topic/group/schema from phases 1-3 already
REM      in place).
REM Phases 2 and 4 need to know, per topic, which consumer groups belong to it; phase 4 also
REM needs, per event, which subject its schema was registered under. Rather than recomputing
REM either (and risking the subject-naming rule in phase 4 drifting from what phase 3 actually
REM registered), phases 2 and 3 record what they did to %GROUPS_MAP_FILE%/%SCHEMA_MAP_FILE%
REM (TEMP files, one "topic|group" / "topic|event|subject" line per entry) and phase 4 reads
REM those back instead of re-deriving anything.
REM
REM Right after phase 3, %SCHEMA_MAP_FILE% is also converted into a proper JSON file at
REM %EVENT_MAP_OUTPUT_FILE% (event-name -> {Topic, Subject}, one entry per registered event) -
REM this is what registration/events-api.ps1 reads (via its -MappingFile) instead of scanning
REM events\ and re-deriving the subject-naming rule itself. This keeps that rule computed in
REM exactly one place (here, in phase 3) rather than two, and lets events-api.ps1 (and its
REM container, which no longer needs events\ mounted in at all) just consume whatever this
REM script most recently registered.
REM
REM Schema Registry subject naming: a topic with exactly one event folder registers its
REM schema under the conventional "<topic>-value" subject (Confluent's default
REM TopicNameStrategy - matches what push-inventory-state-changed.ps1/events-api.ps1
REM assume). A topic with more than one event folder (multiple Avro record types sharing one
REM topic) instead registers each schema under "<topic>-<namespace>.<record-name>"
REM (Confluent's TopicRecordNameStrategy - the documented approach for multiple event types
REM on one topic, since TopicNameStrategy only allows one schema per topic).
REM
REM TODO(ai): unresolved precedence conflict - inventory-events now carries two event
REM folders (InventoryAdjusted, InventoryStateChanged), so InventoryStateChanged's schema
REM registers under "inventory-events-net.pandora.nexus.event.inventory.InventoryStateChanged"
REM instead of the plain "inventory-events-value" subject push-inventory-state-changed.ps1's
REM -SchemaName default and events-api.ps1's "-value"-stripping topic resolution both assume.
REM A human should reconcile this - either update those two scripts'/README.md's defaults to
REM the new subject name, or decide InventoryStateChanged should keep the plain "-value"
REM subject with only the other event(s) disambiguated. This script can't tell which
REM behavior the rest of the tooling should follow, so it picks the Confluent-documented
REM multi-event-per-topic convention consistently rather than special-casing one event.
REM
REM TODO(ai): unresolved precedence conflict - appsettings.Development.json also configures
REM an "inventory-events-consumer" consumer group (plain-JSON KafkaConsumerHostedService) and
REM an "inventory-bulk-import" topic/"inventory-bulk-import-consumer" group
REM (BulkInventoryImportConsumerHostedService), none of which are represented under events\
REM (no consumer-group.json lists "inventory-events-consumer", and inventory-bulk-import has
REM no events\ folder at all since it carries plain JSON, not Avro). The previous, hand-
REM maintained version of this script created all three anyway; this dynamic version, by
REM design, only creates what's discoverable under events\ and no longer creates them. If
REM local testing still needs them, either add an events\inventory-bulk-import\ topic folder
REM (with its own consumer-group.json) and list "inventory-events-consumer" in
REM events\inventory-events\'s consumer-group.json, or confirm dropping them is intentional.
REM
REM Usage: register-defaults.bat [KafkaContainer] [SchemaRegistryHostPort] [RestProxyHostPort]
REM   Defaults: iis-wms-kafka 8085 8086
REM   kube play setup: register-defaults.bat iis-wms-local-kafka-kafka
REM   (find the exact name via "podman ps --filter pod=iis-wms-local-kafka" first - see README.md)

setlocal enabledelayedexpansion

set KAFKA_CONTAINER=%~1
if "%KAFKA_CONTAINER%"=="" set KAFKA_CONTAINER=iis-wms-kafka
set SCHEMA_REGISTRY_PORT=%~2
if "%SCHEMA_REGISTRY_PORT%"=="" set SCHEMA_REGISTRY_PORT=8085
set REST_PROXY_PORT=%~3
if "%REST_PROXY_PORT%"=="" set REST_PROXY_PORT=8086
set CLIENT_CONFIG=/etc/kafka/secrets/client.properties
set "EVENTS_ROOT=%~dp0events"
set "MAPPING_FILE=%TEMP%\iis-wms-defaults-mapping.txt"
set "GROUPS_MAP_FILE=%TEMP%\iis-wms-groups-map.txt"
set "SCHEMA_MAP_FILE=%TEMP%\iis-wms-schema-map.txt"
REM Persistent (not TEMP, not deleted at the end) - this is the artifact events-api.ps1
REM actually consumes. Same "registration\output\" location setup-podman-kafka.bat's
REM containerized events-api.ps1 invocation already bind-mounts in, so nothing extra needs
REM wiring up there.
set "EVENT_MAP_OUTPUT_FILE=%~dp0output\event-map.json"
if exist "%MAPPING_FILE%" del "%MAPPING_FILE%"
if exist "%GROUPS_MAP_FILE%" del "%GROUPS_MAP_FILE%"
if exist "%SCHEMA_MAP_FILE%" del "%SCHEMA_MAP_FILE%"

if not exist "%EVENTS_ROOT%" (
    echo ERROR: "%EVENTS_ROOT%" not found - nothing to register.
    exit /b 1
)

echo(
echo === Checking Kafka REST Proxy reachability (for sample-message publishing) ===
REM Not every setup in this folder runs REST Proxy (the custom-image path deliberately
REM doesn't - see README.md), so this is a soft check: unreachable just skips phase 4 below
REM with a warning, it doesn't fail the whole script. A handful of quick retries covers the
REM case where it's still starting up (the .bat/kube-play setups start it moments before
REM calling this script).
set REST_PROXY_AVAILABLE=0
for /l %%I in (1,1,3) do (
    if !REST_PROXY_AVAILABLE! equ 0 (
        powershell -NoProfile -Command "try { Invoke-RestMethod -Uri 'http://localhost:%REST_PROXY_PORT%/v3/clusters' -TimeoutSec 3 | Out-Null; exit 0 } catch { exit 1 }" >nul 2>&1
        if not errorlevel 1 (
            set REST_PROXY_AVAILABLE=1
        ) else (
            timeout /t 2 >nul
        )
    )
)
if !REST_PROXY_AVAILABLE! equ 0 (
    echo Kafka REST Proxy not reachable at localhost:%REST_PROXY_PORT% - sample messages will NOT be published/consumed ^(topics/consumer groups/schemas are still registered below^). Expected on setups that don't run REST Proxy - see README.md.
) else (
    echo Kafka REST Proxy is reachable - sample messages will be published and consumed in phase 4 below.
)

echo(
echo === Phase 1: Registering topics (discovered under events\) ===
for /d %%T in ("%EVENTS_ROOT%\*") do (
    set "TOPIC=%%~nxT"
    echo Creating topic "!TOPIC!" if missing...
    podman exec %KAFKA_CONTAINER% kafka-topics --bootstrap-server localhost:9092 --command-config %CLIENT_CONFIG% --create --if-not-exists --topic !TOPIC! --partitions 1 --replication-factor 1 || exit /b 1
    echo Topic: !TOPIC! ^(events\!TOPIC!\^)>> "%MAPPING_FILE%"
)

echo(
echo === Phase 2: Registering consumer groups (discovered from each topic's consumer-group.json, "$Default" if none) ===
REM --reset-offsets --execute creates a group's committed-offset entry against a topic
REM without a live consumer ever having to connect first - that's what makes each group
REM show up in Kafka UI's Consumers tab (state: EMPTY) right away, and it's also what phase
REM 4's consume step relies on: without this, kafka-console-consumer would join as a
REM fresh/unknown group instead of advancing this one's committed offset. See
REM :reset_consumer_group below for the delete/settle/reset-offsets sequence.
for /d %%T in ("%EVENTS_ROOT%\*") do (
    set "TOPIC=%%~nxT"
    set "HAS_GROUP_FILE=0"
    for %%G in ("%%T\*.json") do (
        set "HAS_GROUP_FILE=1"
        for /f "usebackq delims=" %%N in (`powershell -NoProfile -Command "$ErrorActionPreference='Stop'; (Get-Content -Raw '%%G' | ConvertFrom-Json) | ForEach-Object { $_ }"`) do (
            echo Registering consumer group "%%N" on topic "!TOPIC!" ^(from %%~nxG^)...
            call :reset_consumer_group "%%N" "!TOPIC!"
            if errorlevel 1 exit /b 1
            echo !TOPIC!^|%%N>> "%GROUPS_MAP_FILE%"
            echo Consumer group: %%N -^> topic !TOPIC! ^(events\!TOPIC!\%%~nxG^)>> "%MAPPING_FILE%"
        )
    )
    REM No direct *.json file under this topic folder to list consumer groups - fall back to
    REM a single "$Default" group rather than leaving the topic with none at all.
    if !HAS_GROUP_FILE! equ 0 (
        echo No consumer-group.json found for topic "!TOPIC!" - registering default consumer group "$Default"...
        call :reset_consumer_group "$Default" "!TOPIC!"
        if errorlevel 1 exit /b 1
        echo !TOPIC!^|$Default>> "%GROUPS_MAP_FILE%"
        echo Consumer group: $Default -^> topic !TOPIC! ^(default - no consumer-group.json found under events\!TOPIC!\^)>> "%MAPPING_FILE%"
    )
)

echo(
echo === Phase 3: Registering schemas (discovered under each topic's event folders) ===
for /d %%T in ("%EVENTS_ROOT%\*") do (
    set "TOPIC=%%~nxT"
    set /a SCHEMA_COUNT=0
    for /d %%E in ("%%T\*") do set /a SCHEMA_COUNT+=1

    for /d %%E in ("%%T\*") do (
        set "EVENT_NAME=%%~nxE"
        set "SCHEMA_FILE="
        set "SCHEMA_FILE_NAME="
        for %%A in ("%%E\*.avsc") do (
            set "SCHEMA_FILE=%%A"
            set "SCHEMA_FILE_NAME=%%~nxA"
        )

        if defined SCHEMA_FILE (
            if !SCHEMA_COUNT! equ 1 (
                set "SUBJECT=!TOPIC!-value"
            ) else (
                for /f "usebackq delims=" %%R in (`powershell -NoProfile -Command "$ErrorActionPreference='Stop'; $j = Get-Content -Raw '!SCHEMA_FILE!' | ConvertFrom-Json; Write-Output ($j.namespace + '.' + $j.name)"`) do set "SUBJECT=!TOPIC!-%%R"
            )

            echo Registering schema for "!EVENT_NAME!" ^(!SCHEMA_FILE!^) under subject "!SUBJECT!"...
            call :register_schema "!SCHEMA_FILE!" "!SUBJECT!"
            if errorlevel 1 exit /b 1

            set "SAMPLE_LIST="
            for %%M in ("%%E\*.json") do set "SAMPLE_LIST=!SAMPLE_LIST! %%~nxM"
            echo Schema: !EVENT_NAME! ^(events\!TOPIC!\!EVENT_NAME!\!SCHEMA_FILE_NAME!^) -^> subject !SUBJECT!, topic !TOPIC!, sample messages:!SAMPLE_LIST!>> "%MAPPING_FILE%"
            echo !TOPIC!^|!EVENT_NAME!^|!SUBJECT!>> "%SCHEMA_MAP_FILE%"
        )
    )
)

echo(
echo === Preparing event/schema/topic mapping JSON for events-api.ps1 ===
if not exist "%~dp0output" mkdir "%~dp0output"
powershell -NoProfile -Command "$ErrorActionPreference='Stop'; $map = [ordered]@{}; if (Test-Path '%SCHEMA_MAP_FILE%') { Get-Content '%SCHEMA_MAP_FILE%' | ForEach-Object { $parts = $_ -split '\|'; if ($parts.Count -ge 3) { $map[$parts[1]] = @{ Topic = $parts[0]; Subject = $parts[2] } } } }; $map | ConvertTo-Json -Depth 5 | Set-Content -Path '%EVENT_MAP_OUTPUT_FILE%' -Encoding ascii" || exit /b 1
echo Wrote "%EVENT_MAP_OUTPUT_FILE%":
type "%EVENT_MAP_OUTPUT_FILE%"

echo(
if !REST_PROXY_AVAILABLE! equ 1 (
    echo === Phase 4: Publishing and consuming sample event messages ===
    for /f "usebackq tokens=1-3 delims=|" %%T in ("%SCHEMA_MAP_FILE%") do (
        set "TOPIC=%%T"
        set "EVENT_NAME=%%U"
        set "SUBJECT=%%V"
        for %%M in ("%EVENTS_ROOT%\!TOPIC!\!EVENT_NAME!\*.json") do (
            echo Publishing sample "%%~nxM" for "!EVENT_NAME!"...
            powershell -NoProfile -File "%~dp0publish-event-sample.ps1" -Topic "!TOPIC!" -Subject "!SUBJECT!" -MessageFile "%%M" -RestProxyUrl "http://localhost:%REST_PROXY_PORT%" -SchemaRegistryUrl "http://localhost:%SCHEMA_REGISTRY_PORT%"
            if errorlevel 1 exit /b 1
            echo Sample: events\!TOPIC!\!EVENT_NAME!\%%~nxM -^> published to topic !TOPIC!>> "%MAPPING_FILE%"

            for /f "usebackq tokens=1,2 delims=|" %%G in ("%GROUPS_MAP_FILE%") do (
                if "%%G"=="!TOPIC!" (
                    echo Consuming it into consumer group "%%H"...
                    podman exec %KAFKA_CONTAINER% kafka-console-consumer --bootstrap-server localhost:9092 --consumer.config %CLIENT_CONFIG% --topic !TOPIC! --group %%H --max-messages 1 --timeout-ms 10000 >nul
                    echo   consumed into group %%H>> "%MAPPING_FILE%"
                )
            )
        )
    )
) else (
    echo === Phase 4 skipped: Kafka REST Proxy not reachable at localhost:%REST_PROXY_PORT% ===
)

echo(
echo Defaults registered:
type "%MAPPING_FILE%"
if !REST_PROXY_AVAILABLE! equ 0 (
    echo(
    echo Sample messages were NOT published/consumed - Kafka REST Proxy was not reachable at localhost:%REST_PROXY_PORT%.
)
del "%MAPPING_FILE%" >nul 2>&1
del "%GROUPS_MAP_FILE%" >nul 2>&1
del "%SCHEMA_MAP_FILE%" >nul 2>&1

endlocal
exit /b 0

REM ============================================================================
REM :reset_consumer_group <GroupName> <Topic>
REM
REM Deletes <GroupName> first if it already exists ("group does not exist" from a group
REM that never existed is expected and ignored - not every group here is guaranteed to
REM pre-exist), retrying the delete itself if Kafka refuses it because the group still has
REM active members ("... could not be deleted due to: GroupNotEmptyException"). Then polls
REM --describe --state until the group isn't reported as mid-rebalance
REM (PreparingRebalance/CompletingRebalance) - --reset-offsets --execute also refuses to
REM touch a group in that state ("Assignments can only be reset if the group ... is
REM inactive"), so running it before the group settles is a race, not just theoretically.
REM Finally re-creates the group's committed offset via --reset-offsets --to-earliest
REM --execute, same as before - this is what makes it show up in Kafka UI (state: EMPTY)
REM without a live consumer ever having to connect.
REM
REM Both wait loops are best-effort with a retry cap, not a hard requirement - if a group
REM still won't settle after retrying, this logs a warning and proceeds anyway rather than
REM failing the whole script, since the subsequent --reset-offsets call will itself fail
REM loudly (and this returns errorlevel 1) if the group is genuinely still unusable.
REM ============================================================================
:reset_consumer_group
setlocal
set "GROUP_NAME=%~1"
set "GROUP_TOPIC=%~2"
set GROUP_CHECK_FILE=%TEMP%\iis-wms-group-check.txt

echo Deleting consumer group "%GROUP_NAME%" if it exists...
set /a DELETE_RETRIES=10
:delete_retry
podman exec %KAFKA_CONTAINER% kafka-consumer-groups --bootstrap-server localhost:9092 --command-config %CLIENT_CONFIG% --delete --group %GROUP_NAME% > "%GROUP_CHECK_FILE%" 2>&1
findstr /C:"not empty" /C:"NotEmptyException" /C:"rebalancing" "%GROUP_CHECK_FILE%" >nul
if not errorlevel 1 (
    set /a DELETE_RETRIES-=1
    if !DELETE_RETRIES! leq 0 (
        echo Warning: "%GROUP_NAME%" still reports active members after retrying - continuing anyway.
        goto delete_done
    )
    timeout /t 2 >nul
    goto delete_retry
)
:delete_done

echo Waiting for consumer group "%GROUP_NAME%" to settle...
set /a STATE_RETRIES=15
:state_wait
podman exec %KAFKA_CONTAINER% kafka-consumer-groups --bootstrap-server localhost:9092 --command-config %CLIENT_CONFIG% --describe --group %GROUP_NAME% --state > "%GROUP_CHECK_FILE%" 2>&1
findstr /C:"PreparingRebalance" /C:"CompletingRebalance" "%GROUP_CHECK_FILE%" >nul
if not errorlevel 1 (
    set /a STATE_RETRIES-=1
    if !STATE_RETRIES! leq 0 (
        echo Warning: "%GROUP_NAME%" did not settle in time - continuing anyway.
        goto state_done
    )
    timeout /t 1 >nul
    goto state_wait
)
:state_done

del "%GROUP_CHECK_FILE%" >nul 2>&1

podman exec %KAFKA_CONTAINER% kafka-consumer-groups --bootstrap-server localhost:9092 --command-config %CLIENT_CONFIG% --group %GROUP_NAME% --topic %GROUP_TOPIC% --reset-offsets --to-earliest --execute
if errorlevel 1 (
    exit /b 1
)
exit /b 0

REM ============================================================================
REM :register_schema <SchemaFilePath> <Subject>
REM
REM Registers <SchemaFilePath>'s contents as a new version of Schema Registry subject
REM <Subject>. Shelled out to PowerShell rather than hand-building the Schema Registry POST
REM body in batch - ConvertTo-Json guarantees correct escaping of the schema's embedded
REM quotes, which would be error-prone to hand-escape as a single cmd.exe argument (same
REM reasoning as the JAAS config files elsewhere in this folder).
REM
REM [string](...) around Get-Content -Raw is required, not decorative: Get-Content wraps
REM its result in a PSObject carrying provider metadata (PSPath/ReadCount/etc.) as extra
REM NoteProperties even though the object still reports System.String - ConvertTo-Json
REM picks up on that decoration and serializes {"schema":{"value":"...", ...}} instead of
REM {"schema":"..."}, which the registry then 400s on since it expects "schema" to be a
REM plain string. The cast strips the decoration back down to a plain string.
REM ============================================================================
:register_schema
setlocal
set "SCHEMA_FILE=%~1"
set "SUBJECT=%~2"
powershell -NoProfile -Command "$ErrorActionPreference='Stop'; $schema = [string](Get-Content -Raw '%SCHEMA_FILE%'); $body = @{ schema = $schema } | ConvertTo-Json -Compress; $auth = [Convert]::ToBase64String([Text.Encoding]::ASCII.GetBytes('schemaregistry:schemaregistry-secret')); try { Invoke-RestMethod -Uri 'http://localhost:%SCHEMA_REGISTRY_PORT%/subjects/%SUBJECT%/versions' -Method Post -ContentType 'application/vnd.schemaregistry.v1+json' -Headers @{ Authorization = ('Basic ' + $auth) } -Body $body | ConvertTo-Json } catch { if ($_.Exception.Response) { $reader = New-Object System.IO.StreamReader($_.Exception.Response.GetResponseStream()); Write-Host $reader.ReadToEnd() } else { Write-Host $_.Exception.Message }; exit 1 }"
if errorlevel 1 (
    echo Failed to register schema "%SUBJECT%" from "%SCHEMA_FILE%" - is the Schema Registry reachable at localhost:%SCHEMA_REGISTRY_PORT%? See the response above for the actual reason.
    exit /b 1
)
exit /b 0
