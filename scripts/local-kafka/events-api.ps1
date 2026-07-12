<#
A thin local HTTP wrapper in front of Kafka REST Proxy's v3 Produce API, so a Postman/curl
caller doesn't have to know about cluster ids, schema ids, or base64-encoding Kafka
headers by hand (see README.md's "Producing a real InventoryStateChanged event" section
for what that looks like without this wrapper).

    POST /api/events?type=<SchemaName>

- Query param "type" is the Schema Registry SUBJECT name (e.g. "inventory-events-value"),
  not the Kafka "Type" header - it's what selects the schema. The Kafka topic is derived
  from it by stripping a trailing "-value" (Confluent's default TopicNameStrategy subject
  naming convention - "inventory-events-value" -> topic "inventory-events"). If your
  subject doesn't follow that convention, this won't resolve the right topic.
- The request BODY is the raw event JSON, forwarded as-is into the v3 API's "value.data" -
  not reparsed/reserialized, so any valid JSON works.
- Every request HEADER you send (other than the usual HTTP/framework ones - Content-Type,
  Content-Length, Host, Connection, Accept*, User-Agent, Cache-Control) is forwarded as a
  Kafka record header, base64-encoded automatically. Set Correlation-Id/Deduplication-Id/
  Type/App-Id (see KafkaHeaderNames.cs) as plain HTTP headers in Postman/curl - no manual
  base64 encoding needed, unlike calling Kafka REST Proxy's v3 API directly.

Run it, then leave it running while you test from Postman/curl:

    powershell -NoProfile -File scripts\local-kafka\events-api.ps1

Ctrl+C to stop. Not started automatically by setup-podman-kafka.bat - it's an interactive
testing aid, not part of the scripted setup.
#>
param(
    [int]$Port = 8087,
    [string]$RestProxyUrl = 'http://localhost:8086',
    [string]$SchemaRegistryUrl = 'http://localhost:8085',
    [string]$SchemaRegistryUsername = 'schemaregistry',
    [string]$SchemaRegistryPassword = 'schemaregistry-secret',
    [string]$KafkaRestContainer = 'iis-wms-kafka-rest'
)

$ErrorActionPreference = 'Stop'

# HTTP/framework headers never meant to become Kafka record headers.
$ExcludedHeaders = @(
    'Content-Type', 'Content-Length', 'Host', 'Connection', 'Accept', 'Accept-Encoding',
    'Accept-Language', 'User-Agent', 'Cache-Control', 'Postman-Token', 'Cookie'
)

function Write-KafkaRestLogs {
    Write-Host "--- podman logs --tail 50 $KafkaRestContainer ---"
    try {
        podman logs --tail 50 $KafkaRestContainer 2>&1 | ForEach-Object { Write-Host $_ }
    } catch {
        Write-Host "(couldn't fetch container logs: $_)"
    }
}

function Send-JsonResponse([System.Net.HttpListenerResponse]$Response, [int]$StatusCode, [string]$Json) {
    $buffer = [Text.Encoding]::UTF8.GetBytes($Json)
    $Response.StatusCode = $StatusCode
    $Response.ContentType = 'application/json'
    $Response.ContentLength64 = $buffer.Length
    $Response.OutputStream.Write($buffer, 0, $buffer.Length)
    $Response.OutputStream.Close()
}

# Cluster id is stable for the lifetime of this broker - resolved once, not per request.
# Schema id is re-resolved on every request instead, since re-registering a newer schema
# version mid-testing-session is exactly the kind of thing this wrapper should pick up
# without a restart.
$script:clusterId = $null
function Get-ClusterId {
    if ($script:clusterId) { return $script:clusterId }
    for ($i = 0; $i -lt 15 -and -not $script:clusterId; $i++) {
        try {
            $clusters = Invoke-RestMethod -Uri "$RestProxyUrl/v3/clusters"
            $script:clusterId = $clusters.data[0].cluster_id
        } catch {
            Start-Sleep -Seconds 2
        }
    }
    if (-not $script:clusterId) {
        throw "Kafka REST Proxy at $RestProxyUrl never became reachable."
    }
    return $script:clusterId
}

$listener = New-Object System.Net.HttpListener
# "localhost" specifically (not "+"/a hostname/a wildcard) is exempt from the URL ACL
# reservation HttpListener otherwise needs admin rights to set up - deliberate, so this
# runs fine as a normal user.
$listener.Prefixes.Add("http://localhost:$Port/")
$listener.Start()

Write-Host "Listening on http://localhost:$Port/api/events?type=<SchemaName> - Ctrl+C to stop."

try {
    while ($listener.IsListening) {
        $context = $listener.GetContext()
        $request = $context.Request
        $response = $context.Response

        try {
            if ($request.HttpMethod -ne 'POST' -or $request.Url.AbsolutePath -ne '/api/events') {
                Send-JsonResponse $response 404 (@{ error = "Unknown route: $($request.HttpMethod) $($request.Url.AbsolutePath) - use POST /api/events?type=<SchemaName>" } | ConvertTo-Json -Compress)
                continue
            }

            $schemaName = $request.QueryString['type']
            if (-not $schemaName) {
                Send-JsonResponse $response 400 (@{ error = "Missing required query parameter 'type' (the Schema Registry subject name)." } | ConvertTo-Json -Compress)
                continue
            }

            $reader = New-Object System.IO.StreamReader($request.InputStream, $request.ContentEncoding)
            $rawBody = $reader.ReadToEnd()
            $reader.Close()

            try {
                $null = $rawBody | ConvertFrom-Json
            } catch {
                Send-JsonResponse $response 400 (@{ error = "Request body is not valid JSON: $($_.Exception.Message)" } | ConvertTo-Json -Compress)
                continue
            }

            # TopicNameStrategy convention - see the header comment for the caveat.
            $topic = $schemaName -replace '-value$', ''

            $kafkaHeaders = @()
            foreach ($headerName in $request.Headers.AllKeys) {
                if ($ExcludedHeaders -contains $headerName) { continue }
                $kafkaHeaders += @{ name = $headerName; value = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($request.Headers[$headerName])) }
            }
            # ConvertTo-Json on a single-element array unwraps it to a bare object, not a
            # one-item JSON array - force it back into array shape either way.
            $headersJson = if ($kafkaHeaders.Count -eq 0) { '[]' } else { "[$(($kafkaHeaders | ForEach-Object { $_ | ConvertTo-Json -Compress }) -join ',')]" }

            Write-Host "POST /api/events?type=$schemaName -> topic=$topic, headers=$($kafkaHeaders.Count)"

            $clusterId = Get-ClusterId
            $srAuth = 'Basic ' + [Convert]::ToBase64String([Text.Encoding]::ASCII.GetBytes("${SchemaRegistryUsername}:${SchemaRegistryPassword}"))

            try {
                $schema = Invoke-RestMethod -Uri "$SchemaRegistryUrl/subjects/$schemaName/versions/latest" -Headers @{ Authorization = $srAuth }
            } catch {
                Send-JsonResponse $response 502 (@{ error = "Couldn't resolve schema for subject '$schemaName' from Schema Registry: $($_.Exception.Message)" } | ConvertTo-Json -Compress)
                continue
            }

            # Same wire-format notes as registration/push-inventory-state-changed.ps1: no
            # explicit "type": "AVRO" (conflicts with schema_id - see that script's
            # comments and confluentinc/kafka-rest#1028), snake_case field names throughout
            # (confirmed against api/v3/openapi.yaml), $rawBody spliced in as-is rather than
            # reparsed/reserialized.
            $produceRequestBody = '{"value":{"schema_id":' + $schema.id + ',"data":' + $rawBody + '},"headers":' + $headersJson + '}'

            $produceResponse = $null
            try {
                $produceResponse = Invoke-RestMethod -Uri "$RestProxyUrl/v3/clusters/$clusterId/topics/$topic/records" -Method Post -ContentType 'application/json' -Body $produceRequestBody
            } catch {
                $statusCode = 502
                $body = $null
                if ($_.ErrorDetails -and $_.ErrorDetails.Message) {
                    $body = $_.ErrorDetails.Message
                } elseif ($_.Exception.Response) {
                    try {
                        $stream = $_.Exception.Response.GetResponseStream()
                        $stream.Position = 0
                        $body = (New-Object System.IO.StreamReader($stream)).ReadToEnd()
                    } catch { }
                }
                Write-KafkaRestLogs
                Send-JsonResponse $response $statusCode (@{ error = 'Kafka REST Proxy call failed'; restProxyResponse = $body; exception = $_.Exception.Message } | ConvertTo-Json -Compress)
                continue
            }

            # The v3 Produce API itself returns HTTP 200 even when a record is rejected -
            # the real outcome is only in the body's error_code/message (same gotcha
            # documented in push-inventory-state-changed.ps1).
            if ($produceResponse.error_code -and $produceResponse.error_code -ne 200) {
                Write-KafkaRestLogs
                Send-JsonResponse $response 502 (@{ error = 'Kafka REST Proxy rejected the record'; restProxyResponse = $produceResponse } | ConvertTo-Json -Depth 10 -Compress)
                continue
            }

            Send-JsonResponse $response 200 ($produceResponse | ConvertTo-Json -Depth 10 -Compress)
        }
        catch {
            Write-Host "Unhandled error: $($_.Exception.Message)"
            try {
                Send-JsonResponse $response 500 (@{ error = $_.Exception.Message } | ConvertTo-Json -Compress)
            } catch { }
        }
    }
}
finally {
    $listener.Stop()
    $listener.Close()
}
