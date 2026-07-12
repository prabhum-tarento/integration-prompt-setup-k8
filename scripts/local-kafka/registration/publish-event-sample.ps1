<#
Publishes one events\<topic>\<event-name>\*.json sample fixture as a real Avro-encoded
Kafka record through Kafka REST Proxy's v3 Produce API (via Schema Registry), the same
wire-format approach push-inventory-state-changed.ps1 uses for its one hardcoded
InventoryStateChanged sample - see that script's comments for the underlying REST Proxy
API quirks (snake_case fields, no explicit "type" alongside schema_id, HTTP 200 on a
rejected record, etc.), not repeated here.

register-defaults.bat calls this once per sample file it discovers under
events\<topic>\<event-name>\, generalizing what setup-podman-kafka.bat used to do only for
the one InventoryStateChanged default - so every event that has both a registered schema
and a sample message gets a real produced-and-consumable test message, not just that one.

A sample fixture file looks like:

    { "headers": { "Content-Type": "...", "Correlation-Id": "...", ... }, "body": { ... } }

Only "headers" is filtered to the four Kafka record headers ConsumerHostedService.cs
actually reads (KafkaHeaderNames.cs: Correlation-Id, Deduplication-Id, Type, App-Id) -
"Content-Type" and anything else under "headers" is fixture documentation, not a Kafka
header, and is dropped.

"body" needs one more transform first: these fixtures are hand-authored for readability, so
any timestamp-millis field (e.g. InventoryAdjusted's adjustment.adjustmentDate,
InventoryStateChanged's changeDate, OrderToInventoryAllocated's allocateDate) is written as
an ISO-8601 string ("2026-07-10T16:22:18.000Z") - but the .avsc declares these fields
{"type": "long", "logicalType": "timestamp-millis"}, and Kafka REST Proxy's v3 JSON->Avro
conversion 400s with "Expected long. Got VALUE_STRING" if handed a string there (confirmed
in push-inventory-state-changed.ps1). This script generically converts every ISO-8601
UTC-timestamp-shaped string anywhere in "body" (recursively, including inside arrays/nested
records) to epoch milliseconds before producing - a heuristic based on this repo's fixture
authoring convention, not real schema-aware conversion: if a future schema ever added a
genuine free-text field whose value happens to match the same shape, it would be converted
too. Acceptable for local test fixtures; revisit if that ever actually happens.
#>
param(
    [Parameter(Mandatory = $true)]
    [string]$Topic,

    # Schema Registry subject to resolve schema_id from - computed by register-defaults.bat
    # using the same TopicNameStrategy/TopicRecordNameStrategy rule it registered the schema
    # under (see that script's header comment), not re-derived here.
    [Parameter(Mandatory = $true)]
    [string]$Subject,

    # Path to the events\<topic>\<event-name>\*.json sample fixture to publish.
    [Parameter(Mandatory = $true)]
    [string]$MessageFile,

    [string]$RestProxyUrl = 'http://localhost:8086',
    [string]$SchemaRegistryUrl = 'http://localhost:8085',
    [string]$SchemaRegistryUsername = 'schemaregistry',
    [string]$SchemaRegistryPassword = 'schemaregistry-secret',
    [string]$KafkaRestContainer = 'iis-wms-kafka-rest'
)

$ErrorActionPreference = 'Stop'

# The four Kafka record headers ConsumerHostedService.cs reads (KafkaHeaderNames.cs) - any
# other key under the fixture's "headers" object (e.g. "Content-Type") is dropped.
$CanonicalHeaderNames = @('Correlation-Id', 'Deduplication-Id', 'Type', 'App-Id')

# Matches this repo's fixture convention for a UTC Avro timestamp-millis value written as
# ISO-8601 - see the header comment for why this is a heuristic, not schema-aware.
$Iso8601UtcPattern = '^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?Z$'

function Write-KafkaRestLogs {
    Write-Host "--- podman logs --tail 50 $KafkaRestContainer ---"
    try {
        podman logs --tail 50 $KafkaRestContainer 2>&1 | ForEach-Object { Write-Host $_ }
    } catch {
        Write-Host "(couldn't fetch container logs: $_)"
    }
}

# Recursively rewrites every ISO-8601 UTC-timestamp-shaped string in $node to epoch
# milliseconds (a JSON number once re-serialized), leaving every other value untouched.
function Convert-TimestampsToEpochMillis {
    param($node)

    if ($null -eq $node) {
        return $node
    }
    if ($node -is [string]) {
        if ($node -match $Iso8601UtcPattern) {
            return [DateTimeOffset]::Parse(
                $node,
                [System.Globalization.CultureInfo]::InvariantCulture,
                [System.Globalization.DateTimeStyles]::AssumeUniversal -bor [System.Globalization.DateTimeStyles]::AdjustToUniversal
            ).ToUnixTimeMilliseconds()
        }
        return $node
    }
    if ($node -is [System.Collections.IEnumerable]) {
        # PowerShell unrolls an array written to the pipeline/returned from a function -
        # for a single-element (or empty) array that makes the caller see a bare scalar
        # (or $null) instead of a 1-element (or empty) array, silently dropping the JSON
        # array wrapper on re-serialization. The unary comma forces this array to survive
        # as one pipeline object instead of being enumerated away.
        $items = @($node | ForEach-Object { Convert-TimestampsToEpochMillis $_ })
        return ,$items
    }
    if ($node -is [System.Management.Automation.PSCustomObject]) {
        $result = [ordered]@{}
        foreach ($property in $node.PSObject.Properties) {
            $result[$property.Name] = Convert-TimestampsToEpochMillis $property.Value
        }
        return [PSCustomObject]$result
    }
    return $node
}

try {
    $sample = Get-Content -Raw $MessageFile | ConvertFrom-Json
} catch {
    throw "'$MessageFile' is not valid JSON: $($_.Exception.Message)"
}
if (-not $sample.PSObject.Properties.Name -contains 'body') {
    throw "'$MessageFile' has no top-level `"body`" property - expected { `"headers`": {...}, `"body`": {...} }."
}

$kafkaHeaders = @()
foreach ($headerName in $CanonicalHeaderNames) {
    if ($sample.headers -and ($sample.headers.PSObject.Properties.Name -contains $headerName)) {
        $value = [string]$sample.headers.$headerName
        $kafkaHeaders += @{ name = $headerName; value = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($value)) }
    }
}
# ConvertTo-Json on a single-element array unwraps it to a bare object, not a one-item JSON
# array - force it back into array shape either way (same fix events-api.ps1 uses).
$headersJson = if ($kafkaHeaders.Count -eq 0) { '[]' } else { "[$(($kafkaHeaders | ForEach-Object { $_ | ConvertTo-Json -Compress }) -join ',')]" }

$bodyForWire = Convert-TimestampsToEpochMillis $sample.body
$bodyJson = $bodyForWire | ConvertTo-Json -Depth 32 -Compress

try {
    # Kafka REST Proxy can take a few seconds after startup before /v3/clusters answers -
    # retry rather than fail on a race (same reasoning/retry shape as
    # push-inventory-state-changed.ps1 and events-api.ps1's Get-ClusterId).
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
    $schema = Invoke-RestMethod -Uri "$SchemaRegistryUrl/subjects/$Subject/versions/latest" -Headers @{ Authorization = $srAuth }

    # No explicit "type": "AVRO" - schema_id alone identifies this as Avro and pairing it
    # with an explicit type 400s (confluentinc/kafka-rest#1028's bug class) - see
    # push-inventory-state-changed.ps1's comments for the confirmed details.
    #
    # "subject" IS required alongside schema_id, though - confirmed via a live 422
    # ("Error serializing message. Error when fetching schema version. subject =
    # <topic>-value") on a TopicRecordNameStrategy subject (a multi-event topic's
    # "<topic>-<namespace>.<name>", not "<topic>-value"): schema_id alone does not skip
    # subject resolution the way omitting "type" does - REST Proxy still needs to fetch a
    # schema *version* to validate against, and without an explicit subject it falls back to
    # guessing the default TopicNameStrategy subject ("<topic>-value"), which doesn't exist
    # for a topic registered under TopicRecordNameStrategy. Passing subject explicitly (the
    # exact one Schema Registry returned $schema.id under, above) makes REST Proxy resolve
    # against that subject instead of guessing.
    $requestBody = '{"value":{"schema_id":' + $schema.id + ',"subject":"' + $Subject.Replace('\', '\\').Replace('"', '\"') + '","data":' + $bodyJson + '},"headers":' + $headersJson + '}'

    Write-Host "POST $RestProxyUrl/v3/clusters/$clusterId/topics/$Topic/records (subject `"$Subject`", from $MessageFile)"

    $response = Invoke-RestMethod -Uri "$RestProxyUrl/v3/clusters/$clusterId/topics/$Topic/records" -Method Post -ContentType 'application/json' -Body $requestBody

    # The v3 Produce API can return HTTP 200 for the call itself while the record still
    # failed - the real outcome is only in the body's error_code/message.
    if ($response.error_code -and $response.error_code -ne 200) {
        Write-KafkaRestLogs
        throw "Kafka REST Proxy accepted the request but rejected the record: error_code=$($response.error_code) message=$($response.message)"
    }

    Write-Host "Produced `"$Topic`" record from $MessageFile (subject `"$Subject`") partition $($response.partition_id) offset $($response.offset)."
}
catch {
    # Same dual-shape error handling as push-inventory-state-changed.ps1 (Windows PowerShell
    # 5.1's WebException vs PowerShell 7+'s HttpResponseException).
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

    Write-Host "Failed to publish '$MessageFile'$(if ($statusCode) { " (HTTP $statusCode)" })."
    if ($responseBody) {
        Write-Host "Response: $responseBody"
    } else {
        Write-Host $_.Exception.Message
    }

    exit 1
}
