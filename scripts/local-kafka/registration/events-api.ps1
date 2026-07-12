<#
A thin local HTTP wrapper in front of Kafka REST Proxy's v3 Produce API, so a Postman/curl
caller doesn't have to know about cluster ids, schema ids, or base64-encoding Kafka
headers by hand (see README.md's "Producing a real InventoryStateChanged event" section
for what that looks like without this wrapper).

    POST /api/events?type=<EventName>

- Query param "type" is an EVENT NAME - the same name as its
  events\<topic>\<event-name>\ folder (e.g. "inventory.InventoryStateChanged",
  "inventory.InventoryAdjusted", "inventory.OrderToInventoryAllocated"), and conventionally
  also the value producers set as the Kafka "Type" header (KafkaHeaderNames.cs) - though
  this query param and that header are resolved independently; set the header yourself if
  you want one (see below). On every request, this looks the event name up in an event-name
  -> {topic, Schema Registry subject} map, resolved one of two ways:
    - -MappingFile (preferred - this is what setup-podman-kafka.bat's containerized
      invocation uses): a pre-built JSON file - register-defaults.bat writes one to
      registration\output\event-map.json every time it runs, using the exact
      TopicNameStrategy/TopicRecordNameStrategy subject each schema was actually registered
      under (see that script's header comment) - so this wrapper never has to re-derive that
      rule itself or see events\ at all, just consume what register-defaults.bat already
      determined.
    - -EventsRoot (fallback when -MappingFile isn't given - the plain host-run default
      below): scans events\<topic>\<event-name>\*.avsc directly and re-derives the same
      subject-naming rule independently. Kept for standalone use without running
      register-defaults.bat first; a mapping file, once available, is preferred so there's
      only one place computing subject names, not two.
  Either way, the map is re-resolved fresh on EVERY request, not cached at startup - so
  regenerating registration\output\event-map.json (by re-running register-defaults.bat) or
  editing events\ takes effect on the very next request, no restart needed. A request for an
  unknown event name gets a 404 listing every event name currently known.
- The request BODY is the raw event JSON, forwarded as-is into the v3 API's "value.data" -
  not reparsed/reserialized, so any valid JSON works.
- Every request HEADER you send (other than the usual HTTP/framework ones - Content-Type,
  Content-Length, Host, Connection, Accept*, User-Agent, Cache-Control) is forwarded as a
  Kafka record header, base64-encoded automatically. Set Correlation-Id/Deduplication-Id/
  Type/App-Id (see KafkaHeaderNames.cs) as plain HTTP headers in Postman/curl - no manual
  base64 encoding needed, unlike calling Kafka REST Proxy's v3 API directly.
- Every request (method/path/headers/body) and every response (status/body) is logged to
  this process's own console output, timestamped - including requests that fail early
  validation (wrong route, missing/unknown "type", invalid JSON body), not just ones that
  reach Kafka REST Proxy.

Run it, then leave it running while you test from Postman/curl:

    powershell -NoProfile -File scripts\local-kafka\registration\events-api.ps1

Ctrl+C to stop. Not started automatically by setup-podman-kafka.bat when run this way -
that script instead runs this same file inside a container as its last step (see its
"Starting events-api.ps1" section), passing -MappingFile so it never mounts/scans events\
at all; running it directly like this (falling back to -EventsRoot) is still fully
supported for anyone who'd rather not containerize it.
#>
param(
    [int]$Port = 8087,
    # HttpListener prefix host. "localhost" (the default) is exempt from the URL ACL
    # reservation HttpListener otherwise needs admin rights to set up on Windows - fine when
    # running directly on a host. Inside a container, a service bound only to "localhost"
    # never sees traffic arriving through a published port (-p 8087:8087 forwards to the
    # container's external interface, not its loopback) - setup-podman-kafka.bat's
    # containerized invocation passes "+" (HttpListener's wildcard-all-interfaces syntax) to
    # work around that; the Windows ACL restriction this default avoids doesn't apply inside
    # a Linux container anyway.
    [string]$ListenHost = 'localhost',
    [string]$RestProxyUrl = 'http://localhost:8086',
    [string]$SchemaRegistryUrl = 'http://localhost:8085',
    [string]$SchemaRegistryUsername = 'schemaregistry',
    [string]$SchemaRegistryPassword = 'schemaregistry-secret',
    [string]$KafkaRestContainer = 'iis-wms-kafka-rest',
    # Fallback source when -MappingFile isn't given - see the header comment.
    [string]$EventsRoot = (Join-Path $PSScriptRoot 'events'),
    # Preferred source: a JSON file shaped { "<event-name>": { "Topic": "...", "Subject":
    # "..." }, ... } - register-defaults.bat writes exactly this to
    # registration\output\event-map.json on every run. Re-read fresh on every request (see
    # the header comment) - not loaded once and cached.
    [string]$MappingFile = ''
)

$ErrorActionPreference = 'Stop'

# Fallback path - re-derives the subject-naming rule directly from events\ when no
# -MappingFile is given. Mirrors register-defaults.bat's own rule exactly, since it has to
# resolve to the same subject that script would register - see the header comment for why
# -MappingFile is preferred over this once it's available.
function Build-EventMapFromEventsFolder([string]$EventsRoot) {
    $map = @{}
    if (-not (Test-Path $EventsRoot)) {
        Write-Host "Warning: events root '$EventsRoot' not found - no events registered."
        return $map
    }
    Get-ChildItem -Path $EventsRoot -Directory | ForEach-Object {
        $topic = $_.Name
        $eventDirs = @(Get-ChildItem -Path $_.FullName -Directory)
        foreach ($eventDir in $eventDirs) {
            $avsc = Get-ChildItem -Path $eventDir.FullName -Filter '*.avsc' | Select-Object -First 1
            if (-not $avsc) { continue }

            if ($eventDirs.Count -eq 1) {
                $subject = "$topic-value"
            } else {
                $schemaJson = Get-Content -Raw $avsc.FullName | ConvertFrom-Json
                $subject = "$topic-$($schemaJson.namespace).$($schemaJson.name)"
            }
            $map[$eventDir.Name] = @{ Topic = $topic; Subject = $subject }
        }
    }
    return $map
}

# Preferred path - reads register-defaults.bat's prepared JSON directly, no folder scan and
# no re-derivation of the subject-naming rule.
function Build-EventMapFromMappingFile([string]$MappingFile) {
    if (-not (Test-Path $MappingFile)) {
        throw "Mapping file '$MappingFile' not found - run register-defaults.bat first to generate it."
    }
    $raw = Get-Content -Raw $MappingFile | ConvertFrom-Json
    $map = @{}
    foreach ($property in $raw.PSObject.Properties) {
        $map[$property.Name] = @{ Topic = $property.Value.Topic; Subject = $property.Value.Subject }
    }
    return $map
}

# Deliberately NOT cached - called fresh on every request (see the header comment for why:
# so a regenerated mapping file/edited events\ folder takes effect without restarting this
# process).
function Get-EventMap {
    if ($MappingFile) {
        return Build-EventMapFromMappingFile $MappingFile
    }
    return Build-EventMapFromEventsFolder $EventsRoot
}

# Startup-only sanity check and log line - NOT a cache. Get-EventMap is called again, fresh,
# for every actual request below.
$startupEventMap = Get-EventMap
$mappingSourceDescription = if ($MappingFile) { "mapping file '$MappingFile'" } else { "events root '$EventsRoot'" }
Write-Host "Discovered $($startupEventMap.Count) event(s) from $mappingSourceDescription`: $($startupEventMap.Keys -join ', ')"

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

# Logs every incoming request (even ones that fail early validation below) before any
# routing/validation happens, so nothing reaching this process goes unlogged.
function Write-RequestLog([System.Net.HttpListenerRequest]$Request, [string]$Body) {
    $timestamp = Get-Date -Format 'yyyy-MM-dd HH:mm:ss'
    $headersText = ($Request.Headers.AllKeys | ForEach-Object { "$($_)=$($Request.Headers[$_])" }) -join '; '
    Write-Host "[$timestamp] REQUEST $($Request.HttpMethod) $($Request.Url.PathAndQuery) headers={$headersText} body=$Body"
}

function Send-JsonResponse([System.Net.HttpListenerResponse]$Response, [int]$StatusCode, [string]$Json) {
    $timestamp = Get-Date -Format 'yyyy-MM-dd HH:mm:ss'
    Write-Host "[$timestamp] RESPONSE $StatusCode $Json"
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
# See -ListenHost's param doc comment above for why this isn't always "localhost".
$listener.Prefixes.Add("http://${ListenHost}:$Port/")
$listener.Start()

# Always displayed as "localhost" here regardless of -ListenHost - this message describes
# how a CALLER (Postman/curl on the host) reaches this wrapper, which is always via
# localhost:$Port whether that's this process's own loopback (plain host run) or a
# container's published port (setup-podman-kafka.bat's containerized run) - not how this
# process itself bound its listening socket.
Write-Host "Listening on http://localhost:$Port/api/events?type=<EventName> - Ctrl+C to stop."

try {
    while ($listener.IsListening) {
        $context = $listener.GetContext()
        $request = $context.Request
        $response = $context.Response

        try {
            $reader = New-Object System.IO.StreamReader($request.InputStream, $request.ContentEncoding)
            $rawBody = $reader.ReadToEnd()
            $reader.Close()

            Write-RequestLog $request $rawBody

            if ($request.HttpMethod -ne 'POST' -or $request.Url.AbsolutePath -ne '/api/events') {
                Send-JsonResponse $response 404 (@{ error = "Unknown route: $($request.HttpMethod) $($request.Url.AbsolutePath) - use POST /api/events?type=<EventName>" } | ConvertTo-Json -Compress)
                continue
            }

            $eventMap = Get-EventMap

            $eventName = $request.QueryString['type']
            if (-not $eventName) {
                Send-JsonResponse $response 400 (@{ error = "Missing required query parameter 'type' (the event name - e.g. one of: $($eventMap.Keys -join ', '))." } | ConvertTo-Json -Compress)
                continue
            }
            if (-not $eventMap.ContainsKey($eventName)) {
                Send-JsonResponse $response 404 (@{ error = "Unknown event '$eventName'. Known events: $($eventMap.Keys -join ', ')" } | ConvertTo-Json -Compress)
                continue
            }
            $topic = $eventMap[$eventName].Topic
            $schemaName = $eventMap[$eventName].Subject

            try {
                $null = $rawBody | ConvertFrom-Json
            } catch {
                Send-JsonResponse $response 400 (@{ error = "Request body is not valid JSON: $($_.Exception.Message)" } | ConvertTo-Json -Compress)
                continue
            }

            $kafkaHeaders = @()
            foreach ($headerName in $request.Headers.AllKeys) {
                if ($ExcludedHeaders -contains $headerName) { continue }
                $kafkaHeaders += @{ name = $headerName; value = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($request.Headers[$headerName])) }
            }
            # ConvertTo-Json on a single-element array unwraps it to a bare object, not a
            # one-item JSON array - force it back into array shape either way.
            $headersJson = if ($kafkaHeaders.Count -eq 0) { '[]' } else { "[$(($kafkaHeaders | ForEach-Object { $_ | ConvertTo-Json -Compress }) -join ',')]" }

            Write-Host "POST /api/events?type=$eventName -> topic=$topic, subject=$schemaName, headers=$($kafkaHeaders.Count)"

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
            #
            # "subject" IS required alongside schema_id, though (see
            # registration/publish-event-sample.ps1's comments for the confirmed 422:
            # "Error when fetching schema version. subject = <topic>-value") - without it,
            # REST Proxy guesses the default TopicNameStrategy subject ("<topic>-value")
            # instead of using $schemaName, which 422s for any event on a topic registered
            # under TopicRecordNameStrategy (more than one event sharing that topic).
            $schemaNameEscaped = $schemaName.Replace('\', '\\').Replace('"', '\"')
            $produceRequestBody = '{"value":{"schema_id":' + $schema.id + ',"subject":"' + $schemaNameEscaped + '","data":' + $rawBody + '},"headers":' + $headersJson + '}'

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
