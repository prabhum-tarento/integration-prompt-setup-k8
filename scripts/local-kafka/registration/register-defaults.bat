@echo off
REM Registers this app's default topics, Avro schema, and consumer groups against an
REM already-running local Kafka + Schema Registry stack, so Kafka UI (http://localhost:8090)
REM shows them immediately instead of only after the real app runs and consumes for the
REM first time. Works against any of the three setups in this folder - pass the Kafka
REM container's actual name if it isn't the .bat/image path's default.
REM
REM Usage: register-defaults.bat [KafkaContainer] [SchemaRegistryHostPort]
REM   Defaults: iis-wms-kafka 8085
REM   kube play setup: register-defaults.bat iis-wms-local-kafka-kafka
REM   (find the exact name via "podman ps --filter pod=iis-wms-local-kafka" first - see README.md)

setlocal enabledelayedexpansion

set KAFKA_CONTAINER=%~1
if "%KAFKA_CONTAINER%"=="" set KAFKA_CONTAINER=iis-wms-kafka
set SCHEMA_REGISTRY_PORT=%~2
if "%SCHEMA_REGISTRY_PORT%"=="" set SCHEMA_REGISTRY_PORT=8085
set CLIENT_CONFIG=/etc/kafka/secrets/client.properties

echo(
echo === Creating default topics ===
podman exec %KAFKA_CONTAINER% kafka-topics --bootstrap-server localhost:9092 --command-config %CLIENT_CONFIG% --create --if-not-exists --topic inventory-events --partitions 1 --replication-factor 1 || exit /b 1
podman exec %KAFKA_CONTAINER% kafka-topics --bootstrap-server localhost:9092 --command-config %CLIENT_CONFIG% --create --if-not-exists --topic inventory-bulk-import --partitions 1 --replication-factor 1 || exit /b 1

echo(
echo === Registering default consumer groups (matching appsettings.json's Kafka:ConsumerGroup values) ===
REM --reset-offsets --execute creates a group's committed-offset entry against a topic
REM without a live consumer ever having to connect first - that's what makes each group
REM show up in Kafka UI's Consumers tab (state: EMPTY) right away. Each group is deleted
REM first if it already exists (harmless if it doesn't - Kafka's own "group does not
REM exist" error is expected and ignored) and its state is polled until it's no longer
REM mid-rebalance, since --reset-offsets refuses to touch a group that isn't fully
REM inactive yet ("Assignments can only be reset if the group ... is inactive") - see
REM :reset_consumer_group below.
call :reset_consumer_group inventory-events-consumer inventory-events
if errorlevel 1 exit /b 1
call :reset_consumer_group $InventoryStateChanged inventory-events
if errorlevel 1 exit /b 1
call :reset_consumer_group inventory-bulk-import-consumer inventory-bulk-import
if errorlevel 1 exit /b 1

echo(
echo === Registering default Avro schema (subject "inventory-events-value") ===
REM Shelled out to PowerShell rather than hand-building the Schema Registry POST body in
REM batch - ConvertTo-Json guarantees correct escaping of the schema's embedded quotes,
REM which would be error-prone to hand-escape as a single cmd.exe argument (same reasoning
REM as the JAAS config files elsewhere in this folder).
REM
REM [string](...) around Get-Content -Raw is required, not decorative: Get-Content wraps
REM its result in a PSObject carrying provider metadata (PSPath/ReadCount/etc.) as extra
REM NoteProperties even though the object still reports System.String - ConvertTo-Json
REM picks up on that decoration and serializes {"schema":{"value":"...", ...}} instead of
REM {"schema":"..."}, which the registry then 400s on since it expects "schema" to be a
REM plain string. The cast strips the decoration back down to a plain string.
powershell -NoProfile -Command "$ErrorActionPreference='Stop'; $schema = [string](Get-Content -Raw '%~dp0inventory-state-changed.avsc'); $body = @{ schema = $schema } | ConvertTo-Json -Compress; $auth = [Convert]::ToBase64String([Text.Encoding]::ASCII.GetBytes('schemaregistry:schemaregistry-secret')); try { Invoke-RestMethod -Uri 'http://localhost:%SCHEMA_REGISTRY_PORT%/subjects/inventory-events-value/versions' -Method Post -ContentType 'application/vnd.schemaregistry.v1+json' -Headers @{ Authorization = ('Basic ' + $auth) } -Body $body | ConvertTo-Json } catch { if ($_.Exception.Response) { $reader = New-Object System.IO.StreamReader($_.Exception.Response.GetResponseStream()); Write-Host $reader.ReadToEnd() } else { Write-Host $_.Exception.Message }; exit 1 }"
if errorlevel 1 (
    echo Failed to register the schema - is the Schema Registry reachable at localhost:%SCHEMA_REGISTRY_PORT%? See the response above for the actual reason.
    exit /b 1
)

echo(
echo Defaults registered:
echo   Topics:          inventory-events, inventory-bulk-import
echo   Consumer groups: inventory-events-consumer, $InventoryStateChanged, inventory-bulk-import-consumer
echo   Schema subject:  inventory-events-value (registration\inventory-state-changed.avsc)

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
set GROUP_NAME=%~1
set GROUP_TOPIC=%~2
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
