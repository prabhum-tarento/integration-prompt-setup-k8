<#
A thin local HTTP wrapper in front of Kafka REST Proxy's v3 Produce API, so a Postman/curl
caller doesn't have to know about cluster ids, schema ids, or base64-encoding Kafka
headers by hand (see README.md's "Producing a real InventoryStateChanged event" section
for what that looks like without this wrapper).

    POST /api/events?type=<EventName>   - publish a test event (see below)
    GET  /logs                          - live request/response log, streamed as plain text
                                           (open it directly in a browser tab and leave it open)
    GET  /api/mapping                   - the current event-name -> {topic, subject} map,
                                           the same one /api/events resolves "type" against
    POST /api/shutdown                  - stop this process (any HTTP method works) - see below

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
  editing events\ takes effect on the very next request, no restart needed. GET /api/mapping
  returns this exact same map, so you can see what "type" values are currently valid without
  guessing from the events\ folder or the JSON file's raw text.
- The request BODY is the raw event JSON, forwarded as-is into the v3 API's "value.data" -
  not reparsed/reserialized, so any valid JSON works.
- Every request HEADER you send (other than the usual HTTP/framework ones - Content-Type,
  Content-Length, Host, Connection, Accept*, User-Agent, Cache-Control) is forwarded as a
  Kafka record header, base64-encoded automatically. Set Correlation-Id/Deduplication-Id/
  Type/App-Id (see KafkaHeaderNames.cs) as plain HTTP headers in Postman/curl - no manual
  base64 encoding needed, unlike calling Kafka REST Proxy's v3 API directly.
- Every request (method/path/headers/body) and every response (status/body) is logged,
  timestamped, to two places: this process's own console output, and GET /logs's live
  stream (see below) - including requests that fail early validation (wrong route,
  missing/unknown "type", invalid JSON body), not just ones that reach Kafka REST Proxy.

GET /logs - open http://localhost:8087/logs directly in a browser tab (or `curl -N`) and
leave it open: it's a plain-text HTTP response that's never closed, with every request/
response log line written and flushed to it as it happens, so the browser tab keeps
growing with new lines in real time - no auto-refresh or JavaScript needed. It replays the
last 500 log lines immediately on connect, then streams new ones as they occur. Closing the
tab (or Ctrl+C on a `curl -N`) disconnects that one viewer only - it doesn't affect this
process or any other request being handled.

POST /api/shutdown - stops this process without needing Ctrl+C in the terminal it was
launched from (which matters most for setup-podman-kafka.bat's containerized invocation,
where the terminal running `podman run` in the foreground is exactly the terminal you'd
otherwise have to Ctrl+C - now `curl -X POST http://localhost:8087/api/shutdown`, or just
opening that URL in a browser, does the same thing from anywhere). Responds once the
shutdown is confirmed, then stops accepting new connections and exits; requests already
in flight when it's called are allowed to finish first.

Concurrency: this can hold a GET /logs connection open indefinitely while still handling
POST /api/events (or another GET /logs, or /api/shutdown) at the same time - a plain
single-threaded accept loop can't do that (a live-forever /logs response would block it from
ever accepting the next connection), so each incoming request is dispatched onto its own
runspace from a RunspacePool as soon as it's accepted; the main thread's only job is to keep
calling GetContext() and immediately hand off. Shared state (the log broadcaster, the
cached Schema Registry cluster id, the shutdown flag) lives in one synchronized hashtable
passed to every dispatched request - see $sync below.

Run it, then leave it running while you test from Postman/curl:

    powershell -NoProfile -File scripts\local-kafka\registration\events-api.ps1

Ctrl+C to stop (or POST /api/shutdown - see above). Not started automatically by
setup-podman-kafka.bat when run this way - that script instead runs this same file inside a
container as its last step (see its "Starting events-api.ps1" section), passing
-MappingFile so it never mounts/scans events\ at all; running it directly like this
(falling back to -EventsRoot) is still fully supported for anyone who'd rather not
containerize it.
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

# ============================================================================
# Functions below run inside EVERY dispatched request's own runspace, not just the main
# thread - each runspace starts with a fresh, empty session, so these have to be explicitly
# hoisted into the RunspacePool's InitialSessionState (see $iss further down) rather than
# just relying on them being "already defined" the way a single-threaded script could.
# Because of that, none of these can close over top-level variables like $MappingFile or
# $RestProxyUrl - every value they need is threaded through explicitly, either as a function
# parameter or via the one $Sync object every request handler receives.
# ============================================================================

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
function Get-EventMap($Sync) {
    if ($Sync.MappingFile) {
        return Build-EventMapFromMappingFile $Sync.MappingFile
    }
    return Build-EventMapFromEventsFolder $Sync.EventsRoot
}

function Write-KafkaRestLogs($Sync) {
    Write-Host "--- podman logs --tail 50 $($Sync.KafkaRestContainer) ---"
    try {
        podman logs --tail 50 $Sync.KafkaRestContainer 2>&1 | ForEach-Object { Write-Host $_ }
    } catch {
        Write-Host "(couldn't fetch container logs: $_)"
    }
}

# Appends to the last-500-lines history GET /logs replays on connect, and pushes to every
# currently-connected /logs viewer's own queue. $Sync.LogLock guards both collections, since
# multiple request runspaces (including multiple /logs viewers registering/unregistering)
# can call this concurrently.
function Publish-LogLine($Sync, [string]$Line) {
    [System.Threading.Monitor]::Enter($Sync.LogLock)
    try {
        $Sync.LogHistory.Add($Line)
        while ($Sync.LogHistory.Count -gt 500) { $Sync.LogHistory.RemoveAt(0) }
        foreach ($subscriber in $Sync.LogSubscribers) {
            $subscriber.Enqueue($Line)
        }
    } finally {
        [System.Threading.Monitor]::Exit($Sync.LogLock)
    }
}

# Logs every incoming request (even ones that fail early validation below) before any
# routing/validation happens, so nothing reaching this process goes unlogged.
function Write-RequestLog($Sync, [System.Net.HttpListenerRequest]$Request, [string]$Body) {
    $timestamp = Get-Date -Format 'yyyy-MM-dd HH:mm:ss'
    $headersText = ($Request.Headers.AllKeys | ForEach-Object { "$($_)=$($Request.Headers[$_])" }) -join '; '
    $line = "[$timestamp] REQUEST $($Request.HttpMethod) $($Request.Url.PathAndQuery) headers={$headersText} body=$Body"
    Write-Host $line
    Publish-LogLine $Sync $line
}

function Send-JsonResponse($Sync, [System.Net.HttpListenerResponse]$Response, [int]$StatusCode, [string]$Json) {
    $timestamp = Get-Date -Format 'yyyy-MM-dd HH:mm:ss'
    $line = "[$timestamp] RESPONSE $StatusCode $Json"
    Write-Host $line
    Publish-LogLine $Sync $line
    $buffer = [Text.Encoding]::UTF8.GetBytes($Json)
    $Response.StatusCode = $StatusCode
    $Response.ContentType = 'application/json'
    $Response.ContentLength64 = $buffer.Length
    $Response.OutputStream.Write($buffer, 0, $buffer.Length)
    $Response.OutputStream.Close()
}

# Cluster id is stable for the lifetime of the broker - resolved once and cached in
# $Sync.ClusterId (shared across every request's runspace), not per request. A benign race
# the first time two requests hit this concurrently before it's cached is fine - both just
# resolve the same value independently. Schema id is still re-resolved on every request
# (see the produce handler below), since re-registering a newer schema version
# mid-testing-session is exactly the kind of thing this wrapper should pick up without a
# restart.
function Get-ClusterId($Sync) {
    if ($Sync.ClusterId) { return $Sync.ClusterId }
    $resolved = $null
    for ($i = 0; $i -lt 15 -and -not $resolved; $i++) {
        try {
            $clusters = Invoke-RestMethod -Uri "$($Sync.RestProxyUrl)/v3/clusters"
            $resolved = $clusters.data[0].cluster_id
        } catch {
            Start-Sleep -Seconds 2
        }
    }
    if (-not $resolved) {
        throw "Kafka REST Proxy at $($Sync.RestProxyUrl) never became reachable."
    }
    $Sync.ClusterId = $resolved
    return $resolved
}

# GET /logs handler - registers its own queue with Publish-LogLine's broadcaster, replays
# recent history, then blocks (in THIS request's own runspace only - the main accept loop is
# free to keep dispatching other requests the whole time) writing new lines as they arrive
# until the client disconnects or the server is shutting down. HttpListenerResponse defaults
# to chunked transfer-encoding as long as ContentLength64 is never set and the caller just
# keeps writing - that's what makes a browser tab opened on this URL keep growing with new
# text instead of waiting for the response to "finish".
function Invoke-LogsStreamRequest($Sync, [System.Net.HttpListenerResponse]$Response) {
    $Response.ContentType = 'text/plain; charset=utf-8'
    $Response.StatusCode = 200
    $queue = New-Object System.Collections.Concurrent.ConcurrentQueue[string]

    [System.Threading.Monitor]::Enter($Sync.LogLock)
    try {
        $Sync.LogSubscribers.Add($queue)
        $historySnapshot = @($Sync.LogHistory)
    } finally {
        [System.Threading.Monitor]::Exit($Sync.LogLock)
    }

    $writer = New-Object System.IO.StreamWriter($Response.OutputStream)
    $writer.AutoFlush = $true
    try {
        $writer.WriteLine('--- connected to events-api.ps1 live log - showing the last {0} line(s), then live ---' -f $historySnapshot.Count)
        foreach ($historyLine in $historySnapshot) { $writer.WriteLine($historyLine) }

        $idleTicks = 0
        while (-not $Sync.ShuttingDown) {
            $line = $null
            if ($queue.TryDequeue([ref]$line)) {
                $writer.WriteLine($line)
                $idleTicks = 0
            } else {
                Start-Sleep -Milliseconds 250
                $idleTicks++
                if ($idleTicks -ge 60) {
                    # ~15s of silence - a harmless heartbeat, mainly so a human watching the
                    # tab can tell this connection is still alive, not stalled.
                    $writer.WriteLine("[$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')] (still listening, no new requests)")
                    $idleTicks = 0
                }
            }
        }
        $writer.WriteLine('--- server is shutting down, closing this stream ---')
    } catch {
        # The client disconnected (browser tab/curl closed) - the next write throws. Normal,
        # not an error worth logging.
    } finally {
        [System.Threading.Monitor]::Enter($Sync.LogLock)
        try { [void]$Sync.LogSubscribers.Remove($queue) } finally { [System.Threading.Monitor]::Exit($Sync.LogLock) }
        try { $Response.OutputStream.Close() } catch { }
    }
}

# POST /api/shutdown (any HTTP method actually works, deliberately - see the header comment:
# this exists so a person can trigger it by just opening the URL in a browser too). Responds
# first, THEN flips $Sync.ShuttingDown and stops the listener - stopping the listener aborts
# its blocked GetContext() call on the main thread (see the main loop below), which is what
# actually ends the process; requests already dispatched to their own runspace (including
# any open /logs streams, which check $Sync.ShuttingDown every ~250ms) get to finish/close
# on their own rather than being cut off mid-response.
function Invoke-ShutdownRequest($Sync, [System.Net.HttpListenerResponse]$Response) {
    Send-JsonResponse $Sync $Response 200 (@{ status = 'shutting down' } | ConvertTo-Json -Compress)
    $Sync.ShuttingDown = $true
    Start-Sleep -Milliseconds 200
    try { $Sync.Listener.Stop() } catch { }
}

# The router every dispatched request runs through, in its own runspace.
function Invoke-ApiRequest($Sync, [System.Net.HttpListenerContext]$Context) {
    $request = $Context.Request
    $response = $Context.Response

    try {
        $path = $request.Url.AbsolutePath

        if ($path -eq '/logs' -and $request.HttpMethod -eq 'GET') {
            # Logged like anything else, but there's no meaningful "body" to log for a GET,
            # and this call doesn't return until the stream ends - no Send-JsonResponse here.
            Write-RequestLog $Sync $request ''
            Invoke-LogsStreamRequest $Sync $response
            return
        }

        $reader = New-Object System.IO.StreamReader($request.InputStream, $request.ContentEncoding)
        $rawBody = $reader.ReadToEnd()
        $reader.Close()

        Write-RequestLog $Sync $request $rawBody

        if ($path -eq '/api/shutdown') {
            Invoke-ShutdownRequest $Sync $response
            return
        }

        if ($path -eq '/api/mapping' -and $request.HttpMethod -eq 'GET') {
            $eventMap = Get-EventMap $Sync
            Send-JsonResponse $Sync $response 200 ($eventMap | ConvertTo-Json -Depth 5)
            return
        }

        # HTTP/framework headers never meant to become Kafka record headers.
        $excludedHeaders = @(
            'Content-Type', 'Content-Length', 'Host', 'Connection', 'Accept', 'Accept-Encoding',
            'Accept-Language', 'User-Agent', 'Cache-Control', 'Postman-Token', 'Cookie'
        )

        if ($request.HttpMethod -ne 'POST' -or $path -ne '/api/events') {
            Send-JsonResponse $Sync $response 404 (@{ error = "Unknown route: $($request.HttpMethod) $path - use POST /api/events?type=<EventName>, GET /logs, GET /api/mapping, or /api/shutdown" } | ConvertTo-Json -Compress)
            return
        }

        $eventMap = Get-EventMap $Sync

        $eventName = $request.QueryString['type']
        if (-not $eventName) {
            Send-JsonResponse $Sync $response 400 (@{ error = "Missing required query parameter 'type' (the event name - e.g. one of: $($eventMap.Keys -join ', '))." } | ConvertTo-Json -Compress)
            return
        }
        if (-not $eventMap.ContainsKey($eventName)) {
            Send-JsonResponse $Sync $response 404 (@{ error = "Unknown event '$eventName'. Known events: $($eventMap.Keys -join ', ')" } | ConvertTo-Json -Compress)
            return
        }
        $topic = $eventMap[$eventName].Topic
        $schemaName = $eventMap[$eventName].Subject

        try {
            $null = $rawBody | ConvertFrom-Json
        } catch {
            Send-JsonResponse $Sync $response 400 (@{ error = "Request body is not valid JSON: $($_.Exception.Message)" } | ConvertTo-Json -Compress)
            return
        }

        $kafkaHeaders = @()
        foreach ($headerName in $request.Headers.AllKeys) {
            if ($excludedHeaders -contains $headerName) { continue }
            $kafkaHeaders += @{ name = $headerName; value = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($request.Headers[$headerName])) }
        }
        # ConvertTo-Json on a single-element array unwraps it to a bare object, not a
        # one-item JSON array - force it back into array shape either way.
        $headersJson = if ($kafkaHeaders.Count -eq 0) { '[]' } else { "[$(($kafkaHeaders | ForEach-Object { $_ | ConvertTo-Json -Compress }) -join ',')]" }

        Write-Host "POST /api/events?type=$eventName -> topic=$topic, subject=$schemaName, headers=$($kafkaHeaders.Count)"

        $clusterId = Get-ClusterId $Sync
        $srAuth = 'Basic ' + [Convert]::ToBase64String([Text.Encoding]::ASCII.GetBytes("$($Sync.SchemaRegistryUsername):$($Sync.SchemaRegistryPassword)"))

        try {
            $schema = Invoke-RestMethod -Uri "$($Sync.SchemaRegistryUrl)/subjects/$schemaName/versions/latest" -Headers @{ Authorization = $srAuth }
        } catch {
            Send-JsonResponse $Sync $response 502 (@{ error = "Couldn't resolve schema for subject '$schemaName' from Schema Registry: $($_.Exception.Message)" } | ConvertTo-Json -Compress)
            return
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
            $produceResponse = Invoke-RestMethod -Uri "$($Sync.RestProxyUrl)/v3/clusters/$clusterId/topics/$topic/records" -Method Post -ContentType 'application/json' -Body $produceRequestBody
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
            Write-KafkaRestLogs $Sync
            Send-JsonResponse $Sync $response $statusCode (@{ error = 'Kafka REST Proxy call failed'; restProxyResponse = $body; exception = $_.Exception.Message } | ConvertTo-Json -Compress)
            return
        }

        # The v3 Produce API itself returns HTTP 200 even when a record is rejected -
        # the real outcome is only in the body's error_code/message (same gotcha
        # documented in push-inventory-state-changed.ps1).
        if ($produceResponse.error_code -and $produceResponse.error_code -ne 200) {
            Write-KafkaRestLogs $Sync
            Send-JsonResponse $Sync $response 502 (@{ error = 'Kafka REST Proxy rejected the record'; restProxyResponse = $produceResponse } | ConvertTo-Json -Depth 10 -Compress)
            return
        }

        Send-JsonResponse $Sync $response 200 ($produceResponse | ConvertTo-Json -Depth 10 -Compress)
    }
    catch {
        Write-Host "Unhandled error: $($_.Exception.Message)"
        try {
            Send-JsonResponse $Sync $response 500 (@{ error = $_.Exception.Message } | ConvertTo-Json -Compress)
        } catch { }
    }
}

# ============================================================================
# Main thread from here on: build the shared state, hoist the functions above into a
# RunspacePool, start the listener, and just keep dispatching.
# ============================================================================

$listener = New-Object System.Net.HttpListener
# See -ListenHost's param doc comment above for why this isn't always "localhost".
$listener.Prefixes.Add("http://${ListenHost}:$Port/")
$listener.Start()

$sync = [hashtable]::Synchronized(@{
    Listener               = $listener
    RestProxyUrl            = $RestProxyUrl
    SchemaRegistryUrl        = $SchemaRegistryUrl
    SchemaRegistryUsername   = $SchemaRegistryUsername
    SchemaRegistryPassword   = $SchemaRegistryPassword
    KafkaRestContainer       = $KafkaRestContainer
    EventsRoot               = $EventsRoot
    MappingFile              = $MappingFile
    ClusterId                = $null
    ShuttingDown             = $false
    LogLock                  = New-Object object
    LogHistory                = New-Object System.Collections.Generic.List[string]
    LogSubscribers           = New-Object System.Collections.Generic.List[object]
})

# Startup-only sanity check and log line - NOT a cache. Get-EventMap is called again, fresh,
# for every actual request (including GET /api/mapping).
$startupEventMap = Get-EventMap $sync
$mappingSourceDescription = if ($MappingFile) { "mapping file '$MappingFile'" } else { "events root '$EventsRoot'" }
Write-Host "Discovered $($startupEventMap.Count) event(s) from $mappingSourceDescription`: $($startupEventMap.Keys -join ', ')"

# Hoists every function defined above into the pool's session state, so a request handled
# in a brand-new runspace (which otherwise starts completely empty - see the big comment at
# the top of the functions section) can still call them by name.
$iss = [System.Management.Automation.Runspaces.InitialSessionState]::CreateDefault()
foreach ($functionName in @(
        'Build-EventMapFromEventsFolder', 'Build-EventMapFromMappingFile', 'Get-EventMap',
        'Write-KafkaRestLogs', 'Publish-LogLine', 'Write-RequestLog', 'Send-JsonResponse',
        'Get-ClusterId', 'Invoke-LogsStreamRequest', 'Invoke-ShutdownRequest', 'Invoke-ApiRequest'
    )) {
    $definition = Get-Item "function:$functionName"
    $iss.Commands.Add((New-Object System.Management.Automation.Runspaces.SessionStateFunctionEntry($definition.Name, $definition.ScriptBlock)))
}

# Bounded rather than unlimited mainly so a burst of requests can't spin up an unbounded
# number of OS threads at once - 20 concurrent in-flight requests (which, for a local
# single-developer testing tool, includes however many /logs tabs someone's left open) is
# generous headroom without being reckless.
$runspacePool = [runspacefactory]::CreateRunspacePool(1, 20, $iss, $Host)
$runspacePool.Open()

# Always displayed as "localhost" here regardless of -ListenHost - this message describes
# how a CALLER (Postman/curl on the host) reaches this wrapper, which is always via
# localhost:$Port whether that's this process's own loopback (plain host run) or a
# container's published port (setup-podman-kafka.bat's containerized run) - not how this
# process itself bound its listening socket.
Write-Host "Listening on http://localhost:$Port - POST /api/events?type=<EventName>, GET /logs, GET /api/mapping, /api/shutdown. Ctrl+C to stop."

$activeHandlers = New-Object System.Collections.Generic.List[object]

try {
    while ($true) {
        try {
            $context = $listener.GetContext()
        } catch {
            # $listener.Stop() (from a /api/shutdown request, handled on a different
            # runspace) aborts a blocked GetContext() call exactly like this - if that's why
            # we're here, exit the loop quietly instead of logging it as a real error. The
            # exact exception type/message .Stop() triggers this with isn't guaranteed to be
            # identical on Windows (HttpListenerException, historically backed by http.sys)
            # vs. Linux/pwsh in a container (the reimplemented, managed HttpListener) - so
            # this checks the shared flag instead of matching on exception type/message,
            # which is portable across both.
            if ($sync.ShuttingDown) { break }
            throw
        }

        $ps = [powershell]::Create()
        $ps.RunspacePool = $runspacePool
        [void]$ps.AddScript({ param($Sync, $Context) Invoke-ApiRequest -Sync $Sync -Context $Context }).AddArgument($sync).AddArgument($context)
        $async = $ps.BeginInvoke()
        $activeHandlers.Add([pscustomobject]@{ PS = $ps; Async = $async })

        # Best-effort cleanup of finished handlers on every iteration, so a long-running
        # session doesn't accumulate one [powershell]/runspace per request forever. Not
        # time-critical - anything still running (e.g. an open /logs stream) is just skipped
        # this time around and picked up on a later iteration once it completes.
        for ($i = $activeHandlers.Count - 1; $i -ge 0; $i--) {
            if ($activeHandlers[$i].Async.IsCompleted) {
                try { $activeHandlers[$i].PS.EndInvoke($activeHandlers[$i].Async) } catch { Write-Host "Request handler error: $($_.Exception.Message)" }
                $activeHandlers[$i].PS.Dispose()
                $activeHandlers.RemoveAt($i)
            }
        }

        if ($sync.ShuttingDown) { break }
    }
}
finally {
    $sync.ShuttingDown = $true
    foreach ($handler in $activeHandlers) {
        try { $handler.PS.Stop() } catch { }
        try { $handler.PS.Dispose() } catch { }
    }
    $runspacePool.Close()
    $runspacePool.Dispose()
    try { $listener.Stop() } catch { }
    $listener.Close()
    Write-Host 'events-api.ps1 stopped.'
}
