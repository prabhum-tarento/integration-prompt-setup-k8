<#
Produces one Avro-encoded event onto a Kafka topic through Kafka REST Proxy's v3 Produce
API (via Schema Registry), with the four Kafka headers KafkaConsumerHostedServiceBase.cs reads (see
WellKnownHeaderNames.cs): Correlation-Id, Deduplication-Id, Type, App-Id.

Generic over the event itself - pass -Body (a raw JSON string, spliced into the request
verbatim, not re-parsed/re-serialized) and -SchemaName (the Schema Registry subject to
resolve, used directly in that lookup's URL) to produce any schema's event; omitting both
falls back to the InventoryStateChanged sample matching
registration/inventory-state-changed.avsc, which is what setup-podman-kafka.bat calls
automatically after register-defaults.bat has registered that schema - see README.md's
"Producing a real InventoryStateChanged event" section for the equivalent manual curl
walkthrough this script automates.
#>
param(
    [string]$RestProxyUrl = 'http://localhost:8086',
    [string]$SchemaRegistryUrl = 'http://localhost:8085',
    [string]$SchemaRegistryUsername = 'schemaregistry',
    [string]$SchemaRegistryPassword = 'schemaregistry-secret',
    [string]$Topic = 'inventory-events',
    # Schema Registry subject to resolve schema_id from - previously hardcoded as
    # "$Topic-value"; now an explicit parameter so this script isn't tied to one topic's
    # naming convention.
    [string]$SchemaName = 'inventory-events-value',
    # Raw JSON for the event payload (the "data" under value) - spliced into the request
    # body as-is, not parsed into a PowerShell object and reserialized, so whatever's
    # passed here reaches Kafka REST Proxy exactly as written (any valid JSON, pretty-
    # printed or compact). Defaults to the InventoryStateChanged sample below if omitted.
    [string]$Body,
    # Kafka "Type" header value (WellKnownHeaderNames.Type) - paired with -SchemaName since
    # each schema this script produces for typically corresponds to one event type.
    [string]$EventType = 'inventory.InventoryStateChanged',
    [string]$EventId = [Guid]::NewGuid().ToString(),
    [string]$CorrelationId = [Guid]::NewGuid().ToString(),
    [string]$DeduplicationId = "dedup-$([Guid]::NewGuid())",
    [string]$AppId = 'setup-podman-kafka.bat',
    [string]$KafkaRestContainer = 'iis-wms-kafka-rest'
)

# error_code/message in the v3 Produce API's response body is deliberately generic (e.g.
# "Error serializing Avro message") and never names the offending field - the actual cause
# (a Java stack trace) only shows up in the REST Proxy container's own stdout, so surface
# that automatically here instead of asking whoever hits this to go run `podman logs`
# themselves and paste it back.
function Write-KafkaRestLogs {
    Write-Host "--- podman logs --tail 50 $KafkaRestContainer ---"
    try {
        podman logs --tail 50 $KafkaRestContainer 2>&1 | ForEach-Object { Write-Host $_ }
    } catch {
        Write-Host "(couldn't fetch container logs: $_)"
    }
}

$ErrorActionPreference = 'Stop'

function ConvertTo-Base64Header([string]$Name, [string]$Value) {
    @{ name = $Name; value = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($Value)) }
}

# Shape matches net.pandora.nexus.event.inventory.InventoryStateChanged (see
# registration/inventory-state-changed.avsc) - one location, an array of itemLines. Only
# used when the caller doesn't pass -Body. changeDate must be epoch milliseconds (a JSON
# number), not an ISO-8601 string - the avsc declares it as {"type": "long", "logicalType":
# "timestamp-millis"} and the REST Proxy 400s with "Expected long. Got VALUE_STRING"
# otherwise.
if (-not $Body) {
    $changeDateMillis = [DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds()
    $Body = @"
{"channel":"OTHER_STORES","id":"$EventId","changeDate":$changeDateMillis,"location":{"id":"TDC","type":"WAREHOUSE"},"entity":null,"type":"BLC","fromState":{"state":"AVAILABLE","status":"PICKABLE"},"toState":{"state":"AVAILABLE","status":"HELD"},"itemLines":[{"lineNum":"1","productId":"294650C01","itemName":null,"quantity":63,"units":null,"countryOfOrigin":"TH","hallmarking":"NON","netWeight":null,"tareWeight":null,"unitPrice":null,"commodityCode":null,"itemCategoryLocalized":null,"itemMaterialNameLocalized":null,"inventoryRegistrationId":null,"customsRegistrationLineNum":null,"isBonded":null}],"referenceId":null}
"@
}

try {
    $null = $Body | ConvertFrom-Json
} catch {
    throw "-Body is not valid JSON: $($_.Exception.Message)"
}

try {
    # Kafka REST Proxy can take a few seconds after startup before /v3/clusters answers -
    # retry rather than fail the whole setup script on a race.
    $clusterId = $null
    for ($i = 0; $i -lt 15 -and -not $clusterId; $i++) {
        try {
            $clusters = Invoke-RestMethod -Uri "$RestProxyUrl/v3/clusters"
            $clusterId = $clusters.data[0].cluster_id
        } catch {
            Start-Sleep -Seconds 2
        }
    }
    if (-not $clusterId) {
        throw "Kafka REST Proxy at $RestProxyUrl never became reachable."
    }

    $srAuth = 'Basic ' + [Convert]::ToBase64String([Text.Encoding]::ASCII.GetBytes("${SchemaRegistryUsername}:${SchemaRegistryPassword}"))
    $schema = Invoke-RestMethod -Uri "$SchemaRegistryUrl/subjects/$SchemaName/versions/latest" -Headers @{ Authorization = $srAuth }

    # "schema_id" (snake_case), confirmed against confluentinc/kafka-rest's own
    # api/v3/openapi.yaml (ProduceRequestData: type/subject/subject_name_strategy/
    # schema_id/schema_version/schema/data) and real captured ProduceResponse examples
    # (partition_id/cluster_id/topic_name/offset/timestamp, all snake_case) - the v3 API is
    # snake_case throughout, including the top-level ProduceRequest's own partition_id. An
    # earlier version of this script used "schemaId"/"partitionId" (camelCase) based on a
    # misreading of a Jackson "unrecognized field" error, which names fields via the Java
    # class's introspected property name, not necessarily the wire JSON name - that was
    # wrong and broke this script; don't re-introduce it without re-checking openapi.yaml.
    #
    # No explicit "type": "AVRO" here - confirmed (via a live 400: "'schema_id=1' cannot be
    # used with 'serializer'") to be the same bug class as confluentinc/kafka-rest#1028
    # ("'schema_version=latest' cannot be used with 'serializer'"): pairing an explicit type
    # with any schema reference conflicts with the serializer the proxy picks internally.
    # schema_id alone already uniquely identifies this as Avro, so type is redundant here -
    # the proxy infers it from schema_id and echoes "type":"AVRO" back in the response.
    #
    # $Body is spliced in as-is (not parsed into a PowerShell object and reserialized via
    # ConvertTo-Json) - it's already a JSON value, and JSON permits arbitrary whitespace
    # between tokens, so a pretty-printed or compact -Body both embed correctly here.
    $headersJson = @(
        (ConvertTo-Base64Header 'Correlation-Id' $CorrelationId)
        (ConvertTo-Base64Header 'Deduplication-Id' $DeduplicationId)
        (ConvertTo-Base64Header 'Type' $EventType)
        (ConvertTo-Base64Header 'App-Id' $AppId)
    ) | ConvertTo-Json -Depth 5 -Compress

    $requestBody = '{"value":{"schema_id":' + $schema.id + ',"data":' + $Body + '},"headers":' + $headersJson + '}'

    Write-Host "POST $RestProxyUrl/v3/clusters/$clusterId/topics/$Topic/records"
    Write-Host "Request body: $requestBody"

    $response = Invoke-RestMethod -Uri "$RestProxyUrl/v3/clusters/$clusterId/topics/$Topic/records" -Method Post -ContentType 'application/json' -Body $requestBody

    Write-Host "Response: $($response | ConvertTo-Json -Depth 10 -Compress)"

    # The v3 Produce API can return HTTP 200 for the call itself while the record still
    # failed (e.g. Avro encoding against a bad schema_id) - the real outcome is only in the
    # body's error_code/message, so Invoke-RestMethod not throwing is not proof of success.
    if ($response.error_code -and $response.error_code -ne 200) {
        Write-KafkaRestLogs
        throw "Kafka REST Proxy accepted the request but rejected the record: error_code=$($response.error_code) message=$($response.message)"
    }

    Write-Host "Produced event id=$EventId to `"$Topic`" (subject `"$SchemaName`") partition $($response.partition_id) offset $($response.offset)."
    Write-Host "  CorrelationId=$CorrelationId  DeduplicationId=$DeduplicationId  Type=$EventType  AppId=$AppId"
}
catch {
    # Invoke-RestMethod's error surface differs between Windows PowerShell 5.1 (a
    # WebException with a readable .Response stream) and PowerShell 7+ (an
    # HttpResponseException where .ErrorDetails.Message already holds the body, and the
    # stream is typically already consumed) - try both rather than assuming one.
    $statusCode = $null
    $responseBody = $null

    if ($_.Exception.Response) {
        $statusCode = [int]$_.Exception.Response.StatusCode
    }

    if ($_.ErrorDetails -and $_.ErrorDetails.Message) {
        $responseBody = $_.ErrorDetails.Message
    } elseif ($_.Exception.Response) {
        try {
            $stream = $_.Exception.Response.GetResponseStream()
            $stream.Position = 0
            $responseBody = (New-Object System.IO.StreamReader($stream)).ReadToEnd()
        } catch {
            $responseBody = $null
        }
    }

    Write-Host "Request failed$(if ($statusCode) { " (HTTP $statusCode)" })."
    if ($responseBody) {
        Write-Host "Response: $responseBody"
    } else {
        Write-Host "No response body available."
        Write-Host $_.Exception.Message
    }

    exit 1
}
