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
REM Usage: setup-podman-kafka.bat [WarehouseId] [Sku] [Quantity] [EventType] [ConsumerGroup]
REM   Defaults: WH1 SKU-123 10 Create inventory-events-consumer
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
REM Publishes two test messages, each consumed into its own consumer group so neither
REM shows up as permanently lagging in Kafka UI: the plain-JSON one above (via
REM kafka-console-producer, consumed into ConsumerGroup), and a second, Avro-encoded
REM InventoryStateChanged event with Correlation-Id/Deduplication-Id/Type/App-Id headers,
REM produced via Kafka REST Proxy's v3 API (see registration/push-inventory-state-changed.ps1)
REM and consumed into the $InventoryStateChanged group.
REM
REM Every container (broker/registry/UI/REST Proxy) is removed and recreated fresh on
REM every run, not reused - see each container's own section below for why. Only the
REM Podman network is reused if it already exists.

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
set NETWORK_NAME=iis-wms-kafka-net
set KAFKA_CONTAINER=iis-wms-kafka
set SCHEMA_REGISTRY_CONTAINER=iis-wms-schema-registry
set KAFKA_UI_CONTAINER=iis-wms-kafka-ui
set KAFKA_REST_CONTAINER=iis-wms-kafka-rest
set TOPIC=inventory-events
set CONFIG_DIR=%~dp0secrets

REM Local-only credentials - not read from Key Vault/user-secrets, this stack never leaves
REM your machine. Change these if you want different local values.
set KAFKA_SASL_USERNAME=kafkaclient
set KAFKA_SASL_PASSWORD=kafkaclient-secret
set SCHEMA_REGISTRY_USERNAME=schemaregistry
set SCHEMA_REGISTRY_PASSWORD=schemaregistry-secret

set WAREHOUSE_ID=%~1
if "%WAREHOUSE_ID%"=="" set WAREHOUSE_ID=WH1
set SKU=%~2
if "%SKU%"=="" set SKU=SKU-123
set QUANTITY=%~3
if "%QUANTITY%"=="" set QUANTITY=10
set EVENT_TYPE=%~4
if "%EVENT_TYPE%"=="" set EVENT_TYPE=Create
set CONSUMER_GROUP=%~5
if "%CONSUMER_GROUP%"=="" set CONSUMER_GROUP=inventory-events-consumer

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
REM has come up repeatedly. Trade-off: unlike REST Proxy, this container has no volume for its
REM topic/message/consumer-offset data, so this also means a fresh, empty broker every
REM run - by design for a local dev/test sandbox (register-defaults.bat and the test
REM messages below recreate everything that matters anyway), not appropriate if you needed
REM data to persist across runs.
podman rm -f %KAFKA_CONTAINER% >nul 2>&1

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
REM Always removed and recreated, same as the broker above - not just for config
REM freshness, but because it MUST be: Schema Registry's own persistent state (every
REM registered schema) lives in the broker's "_schemas" topic, not in this container. Since
REM the broker is now wiped fresh every run, reusing an existing Schema Registry container
REM would leave it pointing at offsets into a "_schemas" topic that no longer exists.
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
echo === Publishing InventoryStateChanged test message (Avro + headers, via REST Proxy) ===
REM Avro-encoded (via Schema Registry) with Correlation-Id/Deduplication-Id/Type/App-Id
REM headers, so the $InventoryStateChanged consumer group has something realistic to
REM actually deserialize and relay to Service Bus - unlike the plain-JSON message above,
REM this one goes through Kafka REST Proxy (localhost:8086), not kafka-console-producer.
REM See registration/push-inventory-state-changed.ps1 and README.md's "Producing a real
REM InventoryStateChanged event" section for the equivalent manual curl walkthrough.
set STATE_CHANGED_EVENT_ID=evt-state-%RANDOM%
powershell -NoProfile -File "%~dp0registration\push-inventory-state-changed.ps1" ^
    -EventId "%STATE_CHANGED_EVENT_ID%" ^
    -CorrelationId "corr-%RANDOM%" ^
    -DeduplicationId "dedup-%STATE_CHANGED_EVENT_ID%" ^
    -AppId "setup-podman-kafka.bat"
if errorlevel 1 (
    echo Failed to publish the InventoryStateChanged test message - see README.md's "Producing a real InventoryStateChanged event" section for troubleshooting.
    exit /b 1
)

echo(
echo === Consuming it into consumer group "$InventoryStateChanged" ===
REM Same reasoning as the plain-JSON message's consume step above - actually commits an
REM offset (register-defaults.bat's earlier --reset-offsets --to-earliest --execute for
REM this group only creates an empty, uncommitted entry - it does NOT consume anything),
REM so Kafka UI shows this group caught up instead of permanently lagging by one message.
REM kafka-console-consumer doesn't understand Avro - it prints the message's raw bytes
REM (magic byte + schema id + Avro binary) as unreadable noise, not the JSON payload; that
REM only affects what's printed here, not the commit. Use Kafka UI's Messages tab (which
REM is Schema-Registry-aware) to see this message decoded properly.
podman exec %KAFKA_CONTAINER% kafka-console-consumer --bootstrap-server localhost:9092 --consumer.config /etc/kafka/secrets/client.properties ^
    --topic %TOPIC% --group $InventoryStateChanged --max-messages 1 --timeout-ms 10000 >nul

echo(
echo Published EventId=%EVENT_ID% WarehouseId=%WAREHOUSE_ID% Sku=%SKU% Quantity=%QUANTITY% EventType=%EVENT_TYPE% to topic "%TOPIC%", consumed into group "%CONSUMER_GROUP%".
echo Also published InventoryStateChanged EventId=%STATE_CHANGED_EVENT_ID% (Avro + headers) to topic "%TOPIC%", consumed into group "$InventoryStateChanged".
echo(
echo Bootstrap servers:  localhost:9092   (Kafka:Username=%KAFKA_SASL_USERNAME%  Kafka:Password=%KAFKA_SASL_PASSWORD%)
echo Schema Registry:    http://localhost:8085   (Kafka:SchemaRegistryApiKey=%SCHEMA_REGISTRY_USERNAME%  Kafka:SchemaRegistryApiSecret=%SCHEMA_REGISTRY_PASSWORD%)
echo Kafka:Protocol should be SaslSsl -^> SaslPlaintext locally (no TLS here), Kafka:AuthenticationMode=Plain.
echo Kafka UI: http://localhost:8090
echo Kafka REST Proxy (push messages via curl): http://localhost:8086 - see README.md "Push a message via curl"

endlocal
