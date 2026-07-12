@echo off
REM Starts a single-node Kafka broker (KRaft, no ZooKeeper) + Schema Registry under Podman,
REM both requiring username/password (SASL/PLAIN on the broker, HTTP Basic on the registry) -
REM matching the credentialed shape of Kafka:Username/Password/SchemaRegistryApiKey/
REM SchemaRegistryApiSecret in src/Api/IIS.WMS.Consumer.Api/appsettings.json, unlike the
REM unauthenticated appsettings.Developments.json profile. BootstrapServers/SchemaRegistryUrl
REM still point at localhost:9092 / http://localhost:8085.
REM
REM Credentials are generated into JAAS/properties files (see %CONFIG_DIR% below) and mounted
REM into the containers, rather than passed as inline -e JAAS strings - avoids the nested
REM quote-escaping that a PlainLoginModule config string (embedded quotes, spaces, semicolons)
REM would otherwise need inside a single cmd.exe argument.
REM
REM Usage: setup-podman-kafka.bat
REM Edit KAFKA_SASL_USERNAME/PASSWORD and SCHEMA_REGISTRY_USERNAME/PASSWORD below to change
REM the local credentials themselves.
REM
REM Also registers this app's default topics/consumer-groups/Avro schema via
REM register-defaults.bat, so Kafka UI (http://localhost:8090) shows them immediately.
REM
REM Starts Kafka UI too, pre-wired to this broker/registry - it runs as its own container
REM on this script's Podman network, not sharing a Pod network namespace the way
REM kafka-pod.yaml's containers do, so it needs its own dedicated listener
REM (SASL_PLAINTEXT_NET) rather than reusing SASL_PLAINTEXT_HOST - see the Kafka broker
REM section below for why.
REM
REM Also starts Kafka REST Proxy, so messages can be pushed with plain curl instead of
REM "podman exec ... kafka-console-producer" - see README.md's "Push a message via curl"
REM section. It reuses the SASL_PLAINTEXT_NET listener the same way Kafka UI does.
REM
REM register-defaults.bat (called below) discovers every topic/consumer-group/schema under
REM registration\events\ and, once it confirms Kafka REST Proxy is reachable, also publishes
REM each event's sample message(s) (Avro-encoded, with headers) and consumes them into that
REM topic's consumer groups - so none of them show up as permanently lagging in Kafka UI.
REM This used to be a separate step here, hardcoded to one InventoryStateChanged sample via
REM registration/push-inventory-state-changed.ps1 - it's now folded into register-defaults.bat
REM and generalized to every discovered event, so it isn't duplicated here.
REM
REM Every container (broker/registry/UI/REST Proxy) is removed and recreated fresh on
REM every run, not reused - see each container's own section below for why. The Podman
REM network and the broker's data volume (%KAFKA_DATA_VOLUME% - topics/messages/consumer
REM offsets/registered schemas) are both reused if they already exist, so that data
REM persists across runs despite the containers themselves always starting fresh.
REM
REM Finally, once everything above is registered and reachable, this launches
REM registration/events-api.ps1 (the interactive Postman/curl testing wrapper - see its own
REM header comment) inside its own container, in the FOREGROUND as the last step - this
REM script's console becomes the one it logs each POST /api/events request to (and response
REM sent back; GET http://localhost:8087/logs shows the same log live in a browser tab
REM instead), and Ctrl+C there stops both it and this script - or, without needing this
REM console at all, POST http://localhost:8087/api/shutdown (or just open that URL) stops
REM events-api.ps1 the same way. Re-run setup-podman-kafka.bat any time to get a fresh stack
REM with the wrapper running again. It runs containerized
REM (rather than via a plain "powershell -File" on the host, like this script used to) so it
REM doesn't depend on pwsh being installed on the host at all - see EVENTS_API_IMAGE below
REM for the image choice. It's also given -MappingFile pointing at event-map.json under
REM %EVENTS_API_OUTPUT_DIR% - register-defaults.bat (called just above) writes that file with
REM every event's topic/schema-subject mapping on every run, so the events-api.ps1 container
REM never needs events\ mounted into it or scanned at all; it just reads what
REM register-defaults.bat already determined, fresh on every request.

setlocal enabledelayedexpansion

set KAFKA_IMAGE=confluentinc/cp-kafka:7.6.0
set SCHEMA_REGISTRY_IMAGE=confluentinc/cp-schema-registry:7.6.0
set KAFKA_UI_IMAGE=docker.io/kafbat/kafka-ui:v1.5.0
REM Kept newer than KAFKA_IMAGE/SCHEMA_REGISTRY_IMAGE (7.6.0) deliberately - Confluent
REM Platform supports running REST Proxy at a different/newer version than the rest of
REM the cluster, and 7.6.0's REST Proxy has a confirmed bug producing Avro values with a
REM logicalType field (e.g. "timestamp-millis"): it converts the JSON value to a
REM java.time.Instant internally, then crashes trying to write that back as a raw long
REM ("ClassCastException: ... cannot be cast to expected type long at
REM InventoryStateChanged.changeDate" in `podman logs iis-wms-kafka-rest`) - see
REM registration/push-inventory-state-changed.ps1. Unverified whether 8.2.2 actually fixes
REM this - if the same ClassCastException still shows up, it hasn't.
set KAFKA_REST_IMAGE=confluentinc/cp-kafka-rest:8.2.2
REM Runs registration/events-api.ps1 (needs pwsh). Microsoft's own current guidance
REM (learn.microsoft.com "Use PowerShell in Docker", checked 2026) is that the old dedicated
REM mcr.microsoft.com/powershell:* images are being deprecated in favor of the .NET SDK
REM images, which bundle pwsh as a preinstalled global tool - "Only the image for the .NET
REM SDK contains PowerShell" per that doc. The alpine variant is used here to keep this
REM sidecar's pull size down; confirmed against dotnet/dotnet-docker's own
REM src/sdk/9.0/alpine3.24/*/Dockerfile that pwsh ships preinstalled at /usr/bin/pwsh in this
REM variant too, not just the default Debian-based one.
set EVENTS_API_IMAGE=mcr.microsoft.com/dotnet/sdk:9.0-alpine3.24
set NETWORK_NAME=iis-wms-kafka-net
set KAFKA_CONTAINER=iis-wms-kafka
set SCHEMA_REGISTRY_CONTAINER=iis-wms-schema-registry
set KAFKA_UI_CONTAINER=iis-wms-kafka-ui
set KAFKA_REST_CONTAINER=iis-wms-kafka-rest
set EVENTS_API_CONTAINER=iis-wms-events-api
REM Bind-mounted into both register-defaults.bat (as its own output\ subfolder, where it
REM writes event-map.json - see that script) and the events-api.ps1 container (read-write,
REM as -MappingFile's source) - this is how the mapping gets from one to the other without
REM the events-api.ps1 container ever needing events\ mounted in itself. Generated, not
REM meant to be committed - see .gitignore.
set EVENTS_API_OUTPUT_DIR=%~dp0registration\output
REM Named Podman volume for the broker's own log dir (/var/lib/kafka/data - topics,
REM messages, consumer offsets, and the KRaft metadata log) - unlike the broker CONTAINER
REM (always removed and recreated below), a named volume survives "podman rm", so this data
REM persists across runs instead of starting empty every time. Schema Registry needs no
REM volume of its own - its state lives entirely in this data (the broker's "_schemas"
REM topic), not in the Schema Registry container.
set KAFKA_DATA_VOLUME=iis-wms-kafka-data
set CONFIG_DIR=%~dp0secrets

REM Local-only credentials - not read from Key Vault/user-secrets, this stack never leaves
REM your machine. Change these if you want different local values.
set KAFKA_SASL_USERNAME=kafkaclient
set KAFKA_SASL_PASSWORD=kafkaclient-secret
set SCHEMA_REGISTRY_USERNAME=schemaregistry
set SCHEMA_REGISTRY_PASSWORD=schemaregistry-secret

echo(
echo === Checking Podman ===
podman version >nul 2>&1
if errorlevel 1 (
    echo Podman is not available on PATH - install/start Podman Desktop first.
    exit /b 1
)

echo(
echo === Generating local credential files ===
if not exist "%CONFIG_DIR%" mkdir "%CONFIG_DIR%"

REM Broker-side SASL/PLAIN identities. "broker" is the broker's own inter-listener identity
REM (required by PlainLoginModule's format even though inter-broker traffic here stays
REM PLAINTEXT); user_%KAFKA_SASL_USERNAME% is the identity clients authenticate as.
> "%CONFIG_DIR%\kafka_server_jaas.conf" (
    echo KafkaServer {
    echo    org.apache.kafka.common.security.plain.PlainLoginModule required
    echo    username="broker"
    echo    password="broker-secret"
    echo    user_broker="broker-secret"
    echo    user_%KAFKA_SASL_USERNAME%="%KAFKA_SASL_PASSWORD%";
    echo };
)

REM Client config for kafka-topics/console-producer/console-consumer run via "podman exec" -
REM they connect to the same SASL_PLAINTEXT_HOST listener as any external client.
> "%CONFIG_DIR%\client.properties" (
    echo security.protocol=SASL_PLAINTEXT
    echo sasl.mechanism=PLAIN
    echo sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="%KAFKA_SASL_USERNAME%" password="%KAFKA_SASL_PASSWORD%";
)

REM Schema Registry's own HTTP Basic Auth (Jetty JAAS + a Jetty-format password file:
REM "username: password,role"), independent of the Kafka SASL credentials above - Confluent
REM Cloud issues separate credentials for the two services and this mirrors that split.
> "%CONFIG_DIR%\schema-registry.jaas" (
    echo SchemaRegistry-Props {
    echo    org.eclipse.jetty.jaas.spi.PropertyFileLoginModule required
    echo    file="/etc/schema-registry/secrets/schema_registry.password"
    echo    debug="false";
    echo };
)

> "%CONFIG_DIR%\schema_registry.password" (
    echo %SCHEMA_REGISTRY_USERNAME%: %SCHEMA_REGISTRY_PASSWORD%,admin
)

REM Kafka UI's env vars, via --env-file rather than a long "-e KEY=value" flag list -
REM KAFKA_CLUSTERS_0_PROPERTIES_SASL_JAAS_CONFIG's value has embedded quotes/spaces/a
REM semicolon, the same escaping hazard the JAAS conf files above avoid by being written
REM to a file instead of a single cmd.exe argument.
> "%CONFIG_DIR%\kafka-ui.env" (
    echo KAFKA_CLUSTERS_0_NAME=iis-wms-local
    echo KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS=%KAFKA_CONTAINER%:9093
    echo KAFKA_CLUSTERS_0_PROPERTIES_SECURITY_PROTOCOL=SASL_PLAINTEXT
    echo KAFKA_CLUSTERS_0_PROPERTIES_SASL_MECHANISM=PLAIN
    echo KAFKA_CLUSTERS_0_PROPERTIES_SASL_JAAS_CONFIG=org.apache.kafka.common.security.plain.PlainLoginModule required username="%KAFKA_SASL_USERNAME%" password="%KAFKA_SASL_PASSWORD%";
    echo KAFKA_CLUSTERS_0_SCHEMAREGISTRY=http://%SCHEMA_REGISTRY_CONTAINER%:8081
    echo KAFKA_CLUSTERS_0_SCHEMAREGISTRYAUTH_USERNAME=%SCHEMA_REGISTRY_USERNAME%
    echo KAFKA_CLUSTERS_0_SCHEMAREGISTRYAUTH_PASSWORD=%SCHEMA_REGISTRY_PASSWORD%
)

REM Kafka REST Proxy's own config file isn't a JAAS/JAAS-adjacent format like the others -
REM these are plain "KAFKA_REST_*" env vars - but it's still written via --env-file for the
REM same reason: KAFKA_REST_CLIENT_SASL_JAAS_CONFIG's value has the same embedded
REM quotes/spaces/semicolon problem. The REST Proxy's own HTTP API has no Basic Auth layer
REM configured here (same call as Kafka UI above) - it authenticates to the *broker* via
REM SASL using these client.* settings, but anyone who can reach localhost:8086 can call it.
> "%CONFIG_DIR%\kafka-rest.env" (
    echo KAFKA_REST_HOST_NAME=%KAFKA_REST_CONTAINER%
    echo KAFKA_REST_BOOTSTRAP_SERVERS=%KAFKA_CONTAINER%:9093
    echo KAFKA_REST_CLIENT_SECURITY_PROTOCOL=SASL_PLAINTEXT
    echo KAFKA_REST_CLIENT_SASL_MECHANISM=PLAIN
    echo KAFKA_REST_CLIENT_SASL_JAAS_CONFIG=org.apache.kafka.common.security.plain.PlainLoginModule required username="%KAFKA_SASL_USERNAME%" password="%KAFKA_SASL_PASSWORD%";
    echo KAFKA_REST_SCHEMA_REGISTRY_URL=http://%SCHEMA_REGISTRY_CONTAINER%:8081
    echo KAFKA_REST_SCHEMA_REGISTRY_BASIC_AUTH_CREDENTIALS_SOURCE=USER_INFO
    echo KAFKA_REST_SCHEMA_REGISTRY_BASIC_AUTH_USER_INFO=%SCHEMA_REGISTRY_USERNAME%:%SCHEMA_REGISTRY_PASSWORD%
    echo KAFKA_REST_LISTENERS=http://0.0.0.0:8082
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
echo === Removing any existing Kafka broker container ===
REM Recreated unconditionally on every run, same reasoning as Schema Registry/Kafka
REM UI/REST Proxy below - guarantees the broker always runs with this script's CURRENT
REM listener/JAAS configuration (e.g. the SASL_PLAINTEXT_NET listener some earlier-created
REM containers predate) instead of silently continuing on a stale one - this exact gotcha
REM has come up repeatedly. This only removes the CONTAINER, not its data - that lives in
REM the %KAFKA_DATA_VOLUME% named volume created below, which this doesn't touch, so topics/
REM messages/consumer offsets/registered schemas persist across runs despite the container
REM itself always starting fresh. Delete that volume yourself (see README.md's Cleanup
REM section) if you want a genuinely empty broker again.
podman rm -f %KAFKA_CONTAINER% >nul 2>&1

echo(
echo === Kafka data volume ===
REM Created once and left alone thereafter - the whole point is that it OUTLIVES the
REM container recreation above. CLUSTER_ID/KAFKA_NODE_ID below are fixed values, so the
REM broker recognizes this volume's existing KRaft metadata on every subsequent run instead
REM of reformatting it.
podman volume inspect %KAFKA_DATA_VOLUME% >nul 2>&1
if errorlevel 1 (
    podman volume create %KAFKA_DATA_VOLUME% || exit /b 1
) else (
    echo %KAFKA_DATA_VOLUME% already exists - broker data will persist from previous runs.
)

echo(
echo === Kafka broker ===
REM CLUSTER_ID is Confluent's own documented example KRaft cluster id - any fixed
REM valid base64 UUID works for a single throwaway local broker.
REM SASL_PLAINTEXT_HOST is the client-facing listener (host port 9092, credential-
REM protected) for the app and CLI tools; PLAINTEXT stays the unauthenticated
REM inter-broker/controller listener. SASL_PLAINTEXT_NET is a *third*, separate
REM listener dedicated to sibling containers on this Podman network (i.e. Kafka UI) -
REM it can't just reuse SASL_PLAINTEXT_HOST, because that listener's advertised address
REM is "localhost:9092": after Kafka UI's initial bootstrap connection (however it
REM reaches the broker), Kafka's metadata response tells it to reconnect to that
REM advertised address - and "localhost" from inside Kafka UI's own container means
REM its own loopback, not the broker. kafka-pod.yaml's containers don't hit this
REM because they share one Pod's network namespace, where "localhost" really is the
REM same place for everyone; these are separate containers, so it isn't here.
podman run -d --name %KAFKA_CONTAINER% --network %NETWORK_NAME% -p 9092:9092 ^
    -v "%CONFIG_DIR%:/etc/kafka/secrets:Z" ^
    -v %KAFKA_DATA_VOLUME%:/var/lib/kafka/data ^
    -e KAFKA_OPTS=-Djava.security.auth.login.config=/etc/kafka/secrets/kafka_server_jaas.conf ^
    -e KAFKA_NODE_ID=1 ^
    -e CLUSTER_ID=MkU3OEVBNTcwNTJENDM2Qk ^
    -e KAFKA_PROCESS_ROLES=broker,controller ^
    -e KAFKA_LISTENERS=PLAINTEXT://0.0.0.0:29092,CONTROLLER://0.0.0.0:29093,SASL_PLAINTEXT_HOST://0.0.0.0:9092,SASL_PLAINTEXT_NET://0.0.0.0:9093 ^
    -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://%KAFKA_CONTAINER%:29092,SASL_PLAINTEXT_HOST://localhost:9092,SASL_PLAINTEXT_NET://%KAFKA_CONTAINER%:9093 ^
    -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,SASL_PLAINTEXT_HOST:SASL_PLAINTEXT,SASL_PLAINTEXT_NET:SASL_PLAINTEXT ^
    -e KAFKA_SASL_ENABLED_MECHANISMS=PLAIN ^
    -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER ^
    -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@%KAFKA_CONTAINER%:29093 ^
    -e KAFKA_INTER_BROKER_LISTENER_NAME=PLAINTEXT ^
    -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 ^
    -e KAFKA_AUTO_CREATE_TOPICS_ENABLE=true ^
    %KAFKA_IMAGE% || exit /b 1

echo(
echo === Schema Registry ===
REM Always removed and recreated, same as the broker above, for config-freshness reasons -
REM not because it has to be: Schema Registry's own persistent state (every registered
REM schema) lives entirely in the broker's "_schemas" topic (now persisted via
REM %KAFKA_DATA_VOLUME%, not this container), so a fresh Schema Registry container picks up
REM every previously registered schema from that topic the moment it starts.
podman rm -f %SCHEMA_REGISTRY_CONTAINER% >nul 2>&1
podman run -d --name %SCHEMA_REGISTRY_CONTAINER% --network %NETWORK_NAME% -p 8085:8081 ^
    -v "%CONFIG_DIR%:/etc/schema-registry/secrets:Z" ^
    -e SCHEMA_REGISTRY_OPTS=-Djava.security.auth.login.config=/etc/schema-registry/secrets/schema-registry.jaas ^
    -e SCHEMA_REGISTRY_AUTHENTICATION_METHOD=BASIC ^
    -e SCHEMA_REGISTRY_AUTHENTICATION_ROLES=admin ^
    -e SCHEMA_REGISTRY_AUTHENTICATION_REALM=SchemaRegistry-Props ^
    -e SCHEMA_REGISTRY_HOST_NAME=%SCHEMA_REGISTRY_CONTAINER% ^
    -e SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS=PLAINTEXT://%KAFKA_CONTAINER%:29092 ^
    -e SCHEMA_REGISTRY_LISTENERS=http://0.0.0.0:8081 ^
    %SCHEMA_REGISTRY_IMAGE% || exit /b 1

echo(
echo === Kafka UI ===
REM Always removed and recreated too - purely stateless (no data of its own worth
REM preserving), so there's no reason not to guarantee it's always current, same as REST
REM Proxy below.
podman rm -f %KAFKA_UI_CONTAINER% >nul 2>&1
podman run -d --name %KAFKA_UI_CONTAINER% --network %NETWORK_NAME% -p 8090:8080 ^
    --env-file "%CONFIG_DIR%\kafka-ui.env" ^
    %KAFKA_UI_IMAGE% || exit /b 1

echo(
echo === Kafka REST Proxy ===
REM Always (re)created fresh, same reasoning as Kafka UI above.
podman rm -f %KAFKA_REST_CONTAINER% >nul 2>&1
podman run -d --name %KAFKA_REST_CONTAINER% --network %NETWORK_NAME% -p 8086:8082 ^
    --env-file "%CONFIG_DIR%\kafka-rest.env" ^
    %KAFKA_REST_IMAGE% || exit /b 1

echo(
echo === Waiting for broker to accept connections ===
set /a RETRIES=30
:wait_broker
podman exec %KAFKA_CONTAINER% kafka-topics --bootstrap-server localhost:9092 --command-config /etc/kafka/secrets/client.properties --list >nul 2>&1
if not errorlevel 1 goto broker_ready
set /a RETRIES-=1
if %RETRIES% leq 0 (
    echo Broker did not become ready in time - check "podman logs %KAFKA_CONTAINER%".
    exit /b 1
)
timeout /t 2 >nul
goto wait_broker
:broker_ready
echo Broker is ready.

call "%~dp0registration\register-defaults.bat" %KAFKA_CONTAINER%
if errorlevel 1 exit /b 1

echo(
echo Bootstrap servers:  localhost:9092   (Kafka:Username=%KAFKA_SASL_USERNAME%  Kafka:Password=%KAFKA_SASL_PASSWORD%)
echo Schema Registry:    http://localhost:8085   (Kafka:SchemaRegistryApiKey=%SCHEMA_REGISTRY_USERNAME%  Kafka:SchemaRegistryApiSecret=%SCHEMA_REGISTRY_PASSWORD%)
echo Kafka:Protocol should be SaslSsl -^> SaslPlaintext locally (no TLS here), Kafka:AuthenticationMode=Plain.
echo Kafka UI: http://localhost:8090
echo Kafka REST Proxy (push messages via curl): http://localhost:8086 - see README.md "Push a message via curl"

echo(
echo === events-api.ps1 output directory ===
if not exist "%EVENTS_API_OUTPUT_DIR%" mkdir "%EVENTS_API_OUTPUT_DIR%"

echo(
echo === Starting events-api.ps1 in a container (Ctrl+C to stop) ===
REM Everything is registered and reachable by this point - hand off to the interactive
REM testing wrapper as the last step, in the foreground, so this script's console becomes
REM the one events-api.ps1 logs each POST /api/events request to (see that script's header
REM comment). Ctrl+C stops it (--rm cleans up the container immediately after) and ends this
REM script; re-run setup-podman-kafka.bat any time to get a fresh stack with the wrapper
REM running again.
REM
REM Runs on %NETWORK_NAME% like every other container here, so it reaches Schema
REM Registry/Kafka REST Proxy by CONTAINER name on their IN-NETWORK ports (8081/8082, not
REM the host-published 8085/8086 - "localhost" from inside this container means its own
REM loopback, not those sibling containers, same reasoning as the Kafka broker section
REM above). -ListenHost + (HttpListener's wildcard-all-interfaces syntax) is required here,
REM unlike the script's own "localhost" default: a service bound only to loopback never sees
REM traffic arriving through -p 8087:8087's published port - see events-api.ps1's -ListenHost
REM doc comment. Only two things are mounted in: the script itself (read-only) and
REM %EVENTS_API_OUTPUT_DIR% (read-write) - events\ itself is NOT mounted here at all;
REM -MappingFile instead points at event-map.json, which register-defaults.bat (called
REM above, before this container starts) already wrote into that same output directory. See
REM events-api.ps1's header comment for why reading that prepared file is preferred over
REM scanning events\ directly. -KafkaRestContainer is still passed for parity with the
REM host-run invocation this replaces, but its one use (Write-KafkaRestLogs's "podman logs"
REM call, on a failed produce) is a no-op in here - this plain dotnet/sdk image has no podman
REM CLI/socket access - already handled gracefully by that function's own try/catch, so a
REM failed produce just won't show container logs.
podman rm -f %EVENTS_API_CONTAINER% >nul 2>&1
podman run --rm --name %EVENTS_API_CONTAINER% --network %NETWORK_NAME% -p 8087:8087 ^
    -v "%~dp0registration\events-api.ps1:/app/events-api.ps1:ro,Z" ^
    -v "%EVENTS_API_OUTPUT_DIR%:/app/output:Z" ^
    %EVENTS_API_IMAGE% pwsh -NoProfile -File /app/events-api.ps1 ^
    -Port 8087 ^
    -ListenHost + ^
    -RestProxyUrl "http://%KAFKA_REST_CONTAINER%:8082" ^
    -SchemaRegistryUrl "http://%SCHEMA_REGISTRY_CONTAINER%:8081" ^
    -SchemaRegistryUsername "%SCHEMA_REGISTRY_USERNAME%" ^
    -SchemaRegistryPassword "%SCHEMA_REGISTRY_PASSWORD%" ^
    -KafkaRestContainer "%KAFKA_REST_CONTAINER%" ^
    -MappingFile /app/output/event-map.json

endlocal
