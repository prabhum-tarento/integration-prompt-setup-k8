<#
.SYNOPSIS
Builds a single HTML page listing every host port and local credential the two Podman setups
under this scripts\ folder (local-emulators, local-kafka) use, plus a live up/down health check
for each one, then opens it in the default browser - so setup-podman-local-stack.bat doesn't
leave you hunting through two scripts' worth of console output for a port, connection-string
value, or whether a given container actually came up.

.DESCRIPTION
Reads the same static config files setup-podman-emulators.bat/setup-podman-kafka.bat themselves
read - ports.env, local-kafka\config\credentials.json, local-emulators\config\cosmos-db-config.json/
service-bus-config.json/storage-config.json - so the values shown reflect what those scripts WILL
use (or already are using). Falls back to the same built-in defaults each .bat script itself falls
back to, for any file/key that's missing, so this never errors out just because a config file was
deleted.

On top of those static values, this now also probes every published port directly over TCP (a
short-timeout socket connect, not an HTTP call) to show a live Up/Down status - and, for the two
services with a documented readiness endpoint this repo's own wait loops already use (the Cosmos DB
Emulator's `/ready`, the Service Bus emulator's `/health`), an extra HTTP call for a real
Ready/"Up but not ready"/Down tri-state instead of just Up/Down. This still never shells out to
`podman` or inspects a container directly - only plain network reachability against localhost - so
it still doesn't need Podman on PATH or any container/network access beyond the same published
ports the app itself would use; it just means the status shown is a snapshot as of generation
time, not a live-updating dashboard - re-run this script (or setup-podman-local-stack.bat) to
refresh it.

The Cosmos DB/Service Bus groups' pills each have a per-group refresh (&#x21bb;) button that
re-checks that one readiness endpoint live from the browser without re-running this script - see
the refreshReadyGroup() script at the bottom of the generated page for why this needs to be opened
as http://localhost:<ServerPort> rather than file://, and what still can't be fixed purely by that
(an emulator not sending CORS headers at all). setup-podman-kafka.bat starts a small container
(dashboard-server.ps1) that serves this exact file at that URL for that reason - see its own
"Opening local stack summary" step. Run standalone (no such container running), this script falls
back to opening the file directly instead, with a warning that the refresh buttons won't work from
there.

.PARAMETER OutFile
Where to write the generated HTML. Defaults to a fixed path under $env:TEMP so re-running this
(once per setup-podman-local-stack.bat run) overwrites the same file/URL instead of littering a
new one per run - not committed to the repo, same reasoning as registration\output\. Pass the same
path setup-podman-kafka.bat's dashboard-server.ps1 container mounts (its own $DASHBOARD_HTML) if
you want a standalone re-run of this script to refresh what that container is already serving.

.PARAMETER ServerPort
The port a dashboard-server.ps1 container (started by setup-podman-kafka.bat) is expected to
already be serving -OutFile's content on. This script itself never starts anything on this port -
it only checks whether something already is (see the launch logic at the bottom) and opens
http://localhost:<ServerPort> if so, falling back to the file directly otherwise. Defaults to 8098,
matching scripts\ports.env's DASHBOARD_PORT default - not read from that file directly since this
script doesn't otherwise depend on scripts\local-kafka's half of ports.env.

.PARAMETER NoLaunch
Skip opening anything in the default browser after writing the file - for anything that just wants
the HTML generated (e.g. a future automated check, or setup-podman-kafka.bat's own call, which
opens the browser itself only once its dashboard-server.ps1 container is confirmed up) without
popping a browser window here.

.EXAMPLE
scripts\generate-local-stack-summary.ps1
#>
[CmdletBinding()]
param(
    [string]$OutFile,
    [int]$ServerPort = 8098,
    [switch]$NoLaunch
)

$ErrorActionPreference = 'Stop'

# See local-emulators\setup-cosmos-data.ps1's own header note on why path-shaped parameters
# default in the body, not in param() - same [CmdletBinding()]/$PSScriptRoot quirk applies here.
if (-not $OutFile) { $OutFile = Join-Path $env:TEMP 'iis-wms-local-stack-summary.html' }

$ScriptsRoot = $PSScriptRoot

function Read-JsonConfig([string]$Path) {
    if (Test-Path $Path) { return (Get-Content -Raw -Path $Path | ConvertFrom-Json) }
    return $null
}

function Value-Or-Default($Value, [string]$Default) {
    if ($Value) { return $Value }
    return $Default
}

# A short-timeout raw socket connect - "is anything listening on this port at all", the same
# minimal signal Azurite's own wait loop in setup-podman-emulators.bat treats as "up" (any
# response, not a specific success status). Deliberately not podman/container-aware - see this
# script's own header comment for why - so a container that exists but crashed on startup, or one
# that was never started via these scripts at all, both just show as Down; this can't and doesn't
# try to distinguish those cases from each other.
function Test-TcpPort([string]$ComputerName, [int]$Port, [int]$TimeoutMs = 800) {
    try {
        $client = [System.Net.Sockets.TcpClient]::new()
        try {
            $iar = $client.BeginConnect($ComputerName, $Port, $null, $null)
            if (-not $iar.AsyncWaitHandle.WaitOne($TimeoutMs)) { return $false }
            $client.EndConnect($iar)
            return $client.Connected
        } finally {
            $client.Close()
        }
    } catch {
        return $false
    }
}

# Only for the two services with a documented, well-known readiness endpoint this repo's own
# setup-podman-emulators.bat wait loops already poll - the Cosmos DB Emulator's /ready and the
# Service Bus emulator's /health - so this can report a real Ready/Down tri-state instead of just
# "something is listening". Every other service below only gets Test-TcpPort above; assuming a
# specific HTTP response shape for services this script doesn't otherwise touch would be guessing.
$IsCoreEdition = $PSVersionTable.PSEdition -eq 'Core'
function Test-HttpReady([string]$Url) {
    try {
        $params = @{ Uri = $Url; Method = 'Get'; TimeoutSec = 2; UseBasicParsing = $true; ErrorAction = 'Stop' }
        if ($IsCoreEdition) {
            $params['SkipCertificateCheck'] = $true
        } else {
            # Windows PowerShell 5.1 has no -SkipCertificateCheck - same self-signed-cert bypass
            # setup-cosmos-data.ps1 already uses for the Cosmos DB Emulator's own HTTPS endpoint,
            # scoped to this process only.
            [Net.ServicePointManager]::ServerCertificateValidationCallback = { $true }
        }
        Invoke-WebRequest @params | Out-Null
        return $true
    } catch {
        return $false
    }
}

function StatusPill([bool]$Up) {
    if ($Up) { return '<span class="pill up">Up</span>' }
    return '<span class="pill down">Down</span>'
}

function ReadyPill([string]$Group, [bool]$PortUp, [bool]$Ready) {
    if ($Ready) { $cls = 'up'; $text = 'Ready' }
    elseif ($PortUp) { $cls = 'warn'; $text = 'Up (not ready)' }
    else { $cls = 'down'; $text = 'Down' }
    # The refresh button re-runs this same readiness check live from the browser (see the
    # refreshReadyGroup() script at the bottom of the page) - data-ready-group ties every pill for
    # the same service (this appears both in a <h2> and in the "All host ports" table) together so
    # one click updates all of them.
    return "<span class=`"pill $cls`" data-ready-group=`"$Group`">$text</span><button type=`"button`" class=`"refresh-btn`" title=`"Re-check $Group health now`" aria-label=`"Re-check $Group health now`" onclick=`"refreshReadyGroup('$Group', this)`">&#x21bb;</button>"
}

# --- Ports (ports.env - shared by both setup-podman-*.bat scripts, see that file's own header
# comment). Defaults here match each .bat script's own built-in defaults exactly. ---
$ports = [ordered]@{
    KAFKA_BROKER_PORT    = '9092'
    SCHEMA_REGISTRY_PORT = '8085'
    KAFKA_UI_PORT        = '8090'
    KAFKA_REST_PORT      = '8086'
    EVENTS_API_PORT      = '8087'
    COSMOS_ENDPOINT_PORT = '8081'
    COSMOS_HEALTH_PORT   = '8080'
    COSMOS_EXPLORER_PORT = '1234'
    SERVICEBUS_AMQP_PORT = '5672'
    SERVICEBUS_MGMT_PORT = '5300'
    AZURITE_BLOB_PORT    = '10000'
    AZURITE_TABLE_PORT   = '10002'
    RCLONE_UI_PORT       = '5572'
}
$portsFile = Join-Path $ScriptsRoot 'ports.env'
if (Test-Path $portsFile) {
    Get-Content $portsFile | ForEach-Object {
        $line = $_.Trim()
        if (-not $line -or $line.StartsWith('#')) { return }
        $parts = $line.Split('=', 2)
        if ($parts.Count -eq 2 -and $ports.Contains($parts[0])) { $ports[$parts[0]] = $parts[1].Trim() }
    }
}

# --- Kafka / Schema Registry / Nexus deduplication credentials
# (local-kafka\config\credentials.json) ---
$kafkaCreds           = Read-JsonConfig (Join-Path $ScriptsRoot 'local-kafka\config\credentials.json')
$kafkaUsername        = Value-Or-Default $kafkaCreds.Kafka.Username 'kafkaclient'
$kafkaPassword         = Value-Or-Default $kafkaCreds.Kafka.Password 'kafkaclient-secret'
$schemaRegistryKey     = Value-Or-Default $kafkaCreds.Kafka.SchemaRegistryApiKey 'schemaregistry'
$schemaRegistrySecret  = Value-Or-Default $kafkaCreds.Kafka.SchemaRegistryApiSecret 'schemaregistry-secret'
$dedupClientId         = Value-Or-Default $kafkaCreds.Nexus.Deduplication.ClientId 'iis-wms-consumer'
$dedupClientSecret     = Value-Or-Default $kafkaCreds.Nexus.Deduplication.ClientSecret 'iis-wms-consumer-secret'

# --- Cosmos DB Emulator master key (local-emulators\config\cosmos-db-config.json) ---
$cosmosConfig = Read-JsonConfig (Join-Path $ScriptsRoot 'local-emulators\config\cosmos-db-config.json')
$cosmosKey = Value-Or-Default $cosmosConfig.emulatorKey 'C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw=='

# --- SQL Server Linux SA password / Service Bus emulator SAS name+key
# (local-emulators\config\service-bus-config.json's "Credentials" key) ---
$sbConfig      = Read-JsonConfig (Join-Path $ScriptsRoot 'local-emulators\config\service-bus-config.json')
$sqlSaPassword = Value-Or-Default $sbConfig.Credentials.Sql.SaPassword 'L0cal-Sb-Emul4tor!'
$sbSasKeyName  = Value-Or-Default $sbConfig.Credentials.ServiceBus.SharedAccessKeyName 'RootManageSharedAccessKey'
$sbSasKey      = Value-Or-Default $sbConfig.Credentials.ServiceBus.SharedAccessKey 'SAS_KEY_VALUE'

# --- Azurite accounts + rclone web UI login (local-emulators\config\storage-config.json) ---
$storageConfig = Read-JsonConfig (Join-Path $ScriptsRoot 'local-emulators\config\storage-config.json')
$accounts = @()
if ($storageConfig -and $storageConfig.accounts) {
    $accounts = if ($storageConfig.accounts -is [array]) { $storageConfig.accounts } else { @($storageConfig.accounts) }
}
$rcloneUiUsername = Value-Or-Default $storageConfig.rcloneUi.username 'rcloneui'
$rcloneUiPassword = Value-Or-Default $storageConfig.rcloneUi.password 'Rcl0ne-Ui-Local!'

# --- Derived endpoints/connection strings - same shape this repo's own appsettings.Development.json
# and each .bat script's console output already use. ---
$kafkaBootstrap    = "localhost:$($ports.KAFKA_BROKER_PORT)"
$schemaRegistryUrl = "http://localhost:$($ports.SCHEMA_REGISTRY_PORT)"
$kafkaUiUrl        = "http://localhost:$($ports.KAFKA_UI_PORT)"
$kafkaRestUrl      = "http://localhost:$($ports.KAFKA_REST_PORT)"
$eventsApiUrl      = "http://localhost:$($ports.EVENTS_API_PORT)"
$cosmosEndpoint    = "https://localhost:$($ports.COSMOS_ENDPOINT_PORT)"
$cosmosExplorerUrl = "http://localhost:$($ports.COSMOS_EXPLORER_PORT)"
$sbAmqpEndpoint    = if ($ports.SERVICEBUS_AMQP_PORT -eq '5672') { 'sb://localhost' } else { "sb://localhost:$($ports.SERVICEBUS_AMQP_PORT)" }
$sbConnectionString = "Endpoint=$sbAmqpEndpoint;SharedAccessKeyName=$sbSasKeyName;SharedAccessKey=$sbSasKey;UseDevelopmentEmulator=true;"
$sbAdminConnectionString = "Endpoint=sb://localhost:$($ports.SERVICEBUS_MGMT_PORT);SharedAccessKeyName=$sbSasKeyName;SharedAccessKey=$sbSasKey;UseDevelopmentEmulator=true;"
$rcloneUiUrl = "http://localhost:$($ports.RCLONE_UI_PORT)"

# --- Live health status - a snapshot as of right now, not a live-updating dashboard (see this
# script's own header comment on why a plain TCP connect, not a podman/container inspection). ---
$portStatus = [ordered]@{}
foreach ($key in $ports.Keys) { $portStatus[$key] = Test-TcpPort 'localhost' ([int]$ports[$key]) }

$cosmosReadyUrl = "http://localhost:$($ports.COSMOS_HEALTH_PORT)/ready"
$sbHealthUrl    = "http://localhost:$($ports.SERVICEBUS_MGMT_PORT)/health"
$cosmosReady = $portStatus.COSMOS_HEALTH_PORT -and (Test-HttpReady $cosmosReadyUrl)
$sbReady     = $portStatus.SERVICEBUS_MGMT_PORT -and (Test-HttpReady $sbHealthUrl)
$azuriteUp   = $portStatus.AZURITE_BLOB_PORT -and $portStatus.AZURITE_TABLE_PORT
$upCount     = @($portStatus.Values | Where-Object { $_ }).Count
$totalCount  = $portStatus.Count

function Esc([string]$Value) {
    if ($null -eq $Value) { return '' }
    return [System.Net.WebUtility]::HtmlEncode($Value)
}

# Pre-signs a read+list Account SAS (Blob service only) per account below, so the "Browse
# containers/blobs" section can list containers/blobs and link straight to each blob for download -
# entirely from this script, at generation time, the same way setup-cosmos-data.ps1 already
# hand-signs its own Cosmos DB REST calls instead of pulling in the Azure.Storage.Blobs SDK
# (CLAUDE.md's "never introduce a new package without approval" rule) or a new container image (an
# earlier attempt at a third-party web-browser blob viewer container - ghcr.io/adrianhall/azurite-ui
# - turned out not to be publicly pullable; this replaces that). See
# learn.microsoft.com/rest/api/storageservices/create-account-sas "Construct the signature string" -
# sv chosen below 2020-12-06 so the string-to-sign has no signedEncryptionScope field to worry about.
function New-BlobAccountSas([string]$AccountName, [string]$AccountKey) {
    $sv = '2020-08-04'
    $expiry = (Get-Date).ToUniversalTime().AddDays(30).ToString('yyyy-MM-ddTHH:mm:ssZ')
    # accountname, signedpermissions, signedservice, signedresourcetype, signedstart (empty),
    # signedexpiry, signedIP (empty), signedProtocol (empty - defaults to "https,http", which is
    # what actually allows our plain http:// calls below), signedversion - in that exact order.
    $fields = @($AccountName, 'rl', 'b', 'sco', '', $expiry, '', '', $sv)
    $stringToSign = ($fields -join "`n") + "`n"
    $hmac = [System.Security.Cryptography.HMACSHA256]::new([Convert]::FromBase64String($AccountKey))
    try {
        $signature = [Convert]::ToBase64String($hmac.ComputeHash([System.Text.Encoding]::UTF8.GetBytes($stringToSign)))
    } finally {
        $hmac.Dispose()
    }
    "sv=$sv&ss=b&srt=sco&sp=rl&se=$([uri]::EscapeDataString($expiry))&sig=$([uri]::EscapeDataString($signature))"
}

# List Containers (learn.microsoft.com/rest/api/storageservices/list-containers2), signed with the
# SAS above. Returns $null (not an empty array) on any failure, so the caller can tell "reachable,
# genuinely empty" from "Azurite isn't up/reachable right now" and show a different message for
# each - same distinction $azuriteUp/$portStatus already draw elsewhere in this script.
function Get-BlobContainerNames([string]$BlobEndpoint, [string]$Sas) {
    try {
        $resp = Invoke-WebRequest -Uri "$BlobEndpoint`?comp=list&$Sas" -Method Get -TimeoutSec 5 -UseBasicParsing -ErrorAction Stop
        $xml = [xml]$resp.Content
        # An empty <Containers/> makes this property $null, not an empty array - piping $null into
        # ForEach-Object still runs the body once with $_ = $null (unlike an empty array, which runs
        # zero times), so this must be checked explicitly instead of relying on @(...| ForEach-Object)
        # alone - same single-vs-empty coercion setup-storage-data.ps1's own header comment already
        # calls out for ConvertFrom-Json, just hitting XML nodes here instead of JSON arrays.
        $nodes = $xml.EnumerationResults.Containers.Container
        if ($null -eq $nodes) { return @() }
        if ($nodes -isnot [array]) { $nodes = @($nodes) }
        return @($nodes | ForEach-Object { $_.Name })
    } catch {
        return $null
    }
}

# List Blobs (learn.microsoft.com/rest/api/storageservices/list-blobs), capped at 100 per container -
# large enough for local dev/test data, small enough to keep the generated page a reasonable size;
# Truncated below reflects whether more exist, so the page says so instead of silently looking
# complete. Same $null-on-failure convention as Get-BlobContainerNames above.
function Get-BlobNames([string]$BlobEndpoint, [string]$Container, [string]$Sas) {
    try {
        $resp = Invoke-WebRequest -Uri "$BlobEndpoint/$Container`?restype=container&comp=list&maxresults=100&$Sas" -Method Get -TimeoutSec 5 -UseBasicParsing -ErrorAction Stop
        $xml = [xml]$resp.Content
        $truncated = -not [string]::IsNullOrEmpty([string]$xml.EnumerationResults.NextMarker)
        # See Get-BlobContainerNames' own comment above on why $null must be checked explicitly here
        # instead of piping straight into ForEach-Object.
        $nodes = $xml.EnumerationResults.Blobs.Blob
        if ($null -eq $nodes) { $nodes = @() }
        elseif ($nodes -isnot [array]) { $nodes = @($nodes) }
        $blobs = @($nodes | ForEach-Object {
            [pscustomobject]@{
                Name         = $_.Name
                Size         = [int64]$_.Properties.'Content-Length'
                LastModified = $_.Properties.'Last-Modified'
            }
        })
        [pscustomobject]@{ Blobs = $blobs; Truncated = $truncated }
    } catch {
        return $null
    }
}

function Format-ByteSize([int64]$Bytes) {
    if ($Bytes -ge 1MB) { return "{0:N1} MB" -f ($Bytes / 1MB) }
    if ($Bytes -ge 1KB) { return "{0:N1} KB" -f ($Bytes / 1KB) }
    return "$Bytes B"
}

# EscapeDataString would also encode a blob name's own "/" separators (valid in Azure Blob names,
# denoting a virtual directory) as %2F, breaking the path - escape each segment, not the whole name.
function ConvertTo-UrlBlobPath([string]$BlobName) {
    ($BlobName -split '/' | ForEach-Object { [uri]::EscapeDataString($_) }) -join '/'
}

$accountRows = ''
$accountBrowsers = ''
foreach ($account in $accounts) {
    $name = $account.name
    $key = $account.key
    $containerList = if ($account.containers) { ($account.containers -join ', ') } else { '(none)' }
    $blobConn = "DefaultEndpointsProtocol=http;AccountName=$name;AccountKey=$key;BlobEndpoint=http://localhost:$($ports.AZURITE_BLOB_PORT)/$name;TableEndpoint=http://localhost:$($ports.AZURITE_TABLE_PORT)/$name;"
    $accountRows += (@"
    <tr><td>$(Esc $name)</td><td>$(StatusPill $azuriteUp)</td><td class="mono">$(Esc $key)</td><td>$(Esc $containerList)</td><td class="mono wrap">$(Esc $blobConn)</td></tr>
"@) + "`n"

    $localBlobEndpoint = "http://localhost:$($ports.AZURITE_BLOB_PORT)/$name"
    $sas = New-BlobAccountSas $name $key
    $containerNames = if ($azuriteUp) { Get-BlobContainerNames $localBlobEndpoint $sas } else { $null }

    if ($null -eq $containerNames) {
        $accountBrowsers += "<details><summary><strong>$(Esc $name)</strong></summary><p class=`"note`">Azurite isn't reachable right now - re-run this script once it's up to browse this account's containers/blobs.</p></details>`n"
        continue
    }
    if ($containerNames.Count -eq 0) {
        $accountBrowsers += "<details><summary><strong>$(Esc $name)</strong></summary><p class=`"note`">No containers on this account yet.</p></details>`n"
        continue
    }

    $containerBlocks = ($containerNames | ForEach-Object {
        $containerName = $_
        $result = Get-BlobNames $localBlobEndpoint $containerName $sas
        if ($null -eq $result) {
            "<details><summary>$(Esc $containerName)</summary><p class=`"note`">Couldn't list this container just now.</p></details>"
        } elseif ($result.Blobs.Count -eq 0) {
            "<details><summary>$(Esc $containerName) (empty)</summary></details>"
        } else {
            $blobRows = ($result.Blobs | ForEach-Object {
                $downloadUrl = "$localBlobEndpoint/$containerName/$(ConvertTo-UrlBlobPath $_.Name)?$sas"
                "<tr><td>$(Esc $_.Name)</td><td>$(Format-ByteSize $_.Size)</td><td>$(Esc $_.LastModified)</td><td><a href=`"$(Esc $downloadUrl)`">Download</a></td></tr>"
            }) -join "`n"
            $truncNote = if ($result.Truncated) { '<p class="note">Showing the first 100 blobs only - use Azure Storage Explorer (below) to see the rest.</p>' } else { '' }
            @"
<details><summary>$(Esc $containerName) ($($result.Blobs.Count) blob$(if ($result.Blobs.Count -ne 1) { 's' }))</summary>
<table><tr><th>Name</th><th>Size</th><th>Last modified</th><th></th></tr>
$blobRows
</table>
$truncNote
</details>
"@
        }
    }) -join "`n"

    $accountBrowsers += "<details open><summary><strong>$(Esc $name)</strong></summary>$containerBlocks</details>`n"
}

$generatedAt = Get-Date -Format 'yyyy-MM-dd HH:mm:ss'

$html = @"
<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>iis-wms local stack - ports &amp; credentials</title>
<style>
  :root { color-scheme: light dark; }
  body { font-family: -apple-system, Segoe UI, Roboto, sans-serif; max-width: 960px; margin: 2rem auto; padding: 0 1rem; line-height: 1.5; }
  h1 { font-size: 1.4rem; }
  h2 { font-size: 1.1rem; margin-top: 2.2rem; border-bottom: 1px solid rgba(128,128,128,.35); padding-bottom: .3rem; }
  table { width: 100%; border-collapse: collapse; margin: .75rem 0 1.5rem; font-size: .92rem; }
  th, td { text-align: left; padding: .4rem .6rem; border-bottom: 1px solid rgba(128,128,128,.25); vertical-align: top; }
  th { font-weight: 600; }
  td.mono, code, pre { font-family: Consolas, "SFMono-Regular", Menlo, monospace; font-size: .85rem; }
  td.wrap { word-break: break-all; }
  .note { opacity: .75; font-size: .85rem; }
  .pill { display: inline-block; padding: .1rem .5rem; border-radius: 1rem; background: rgba(128,128,128,.18); font-size: .8rem; }
  .pill.up { background: rgba(34,197,94,.22); color: #15803d; }
  .pill.down { background: rgba(239,68,68,.22); color: #b91c1c; }
  .pill.warn { background: rgba(234,179,8,.25); color: #a16207; }
  @media (prefers-color-scheme: dark) {
    .pill.up { color: #4ade80; }
    .pill.down { color: #f87171; }
    .pill.warn { color: #facc15; }
  }
  .refresh-btn { border: none; background: transparent; cursor: pointer; font-size: 1rem; line-height: 1; padding: 0 .2rem; vertical-align: middle; color: inherit; opacity: .7; }
  .refresh-btn:hover:not(:disabled) { opacity: 1; }
  .refresh-btn:disabled { cursor: default; opacity: .4; }
  .refresh-btn.spinning { display: inline-block; animation: refresh-spin .8s linear infinite; }
  @keyframes refresh-spin { from { transform: rotate(0deg); } to { transform: rotate(360deg); } }
  .callout { border: 1px solid rgba(128,128,128,.3); border-radius: .5rem; padding: .75rem 1rem; margin: .75rem 0 1.5rem; font-size: .9rem; }
  .callout ol { margin: .5rem 0 0; padding-left: 1.25rem; }
  a { color: inherit; }
  h3 { font-size: 1rem; margin-top: 1.5rem; }
  details { margin: .3rem 0; }
  details > summary { cursor: pointer; padding: .2rem 0; }
  details details { margin-left: 1.25rem; }
  details table { margin: .4rem 0 .6rem; }
</style>
</head>
<body>
<h1>iis-wms local stack - ports &amp; credentials</h1>
<p class="note">Generated $generatedAt by <code>scripts\generate-local-stack-summary.ps1</code>. Values come
from <code>scripts\ports.env</code>, <code>scripts\local-kafka\config\credentials.json</code>, and
<code>scripts\local-emulators\config\*.json</code> - re-run <code>setup-podman-local-stack.bat</code>
(or this script directly) after editing any of those to refresh this page. These are local-only,
throwaway values - never a real Azure/Confluent credential.</p>
<p><strong>$upCount / $totalCount</strong> published ports responding right now (see the Status column
in each table below, and "All host ports" for the full list) - a snapshot as of $generatedAt, not
live-updating; re-run this script to refresh it.</p>

<h2>Kafka + Schema Registry + Kafka UI + REST Proxy <span class="pill">local-kafka</span></h2>
<table>
  <tr><th>Setting</th><th>Value</th></tr>
  <tr><td>Kafka:BootstrapServers</td><td class="mono">$(Esc $kafkaBootstrap)</td></tr>
  <tr><td>Kafka:Protocol</td><td>SaslPlaintext</td></tr>
  <tr><td>Kafka:AuthenticationMode</td><td>Plain</td></tr>
  <tr><td>Kafka:Username</td><td class="mono">$(Esc $kafkaUsername)</td></tr>
  <tr><td>Kafka:Password</td><td class="mono">$(Esc $kafkaPassword)</td></tr>
  <tr><td>Kafka:SchemaRegistryUrl</td><td class="mono">$(Esc $schemaRegistryUrl)</td></tr>
  <tr><td>Kafka:SchemaRegistryApiKey</td><td class="mono">$(Esc $schemaRegistryKey)</td></tr>
  <tr><td>Kafka:SchemaRegistryApiSecret</td><td class="mono">$(Esc $schemaRegistrySecret)</td></tr>
  <tr><td>Kafka UI</td><td><a href="$(Esc $kafkaUiUrl)">$(Esc $kafkaUiUrl)</a> $(StatusPill $portStatus.KAFKA_UI_PORT)</td></tr>
  <tr><td>Kafka REST Proxy</td><td><a href="$(Esc $kafkaRestUrl)">$(Esc $kafkaRestUrl)</a> $(StatusPill $portStatus.KAFKA_REST_PORT)</td></tr>
  <tr><td>events-api.ps1 wrapper</td><td><a href="$(Esc $eventsApiUrl)">$(Esc $eventsApiUrl)</a> $(StatusPill $portStatus.EVENTS_API_PORT) (only up once <code>setup-podman-kafka.bat</code> reaches its last step)</td></tr>
  <tr><td>Kafka broker</td><td>$(StatusPill $portStatus.KAFKA_BROKER_PORT)</td></tr>
</table>

<h2>Nexus deduplication mock <span class="pill">local-kafka</span></h2>
<table>
  <tr><th>Setting</th><th>Value</th></tr>
  <tr><td>Nexus:Deduplication:BaseUrl / OAuthEndpoint</td><td class="mono">$(Esc $eventsApiUrl)/</td></tr>
  <tr><td>Nexus:Deduplication:ClientId</td><td class="mono">$(Esc $dedupClientId)</td></tr>
  <tr><td>Nexus:Deduplication:ClientSecret</td><td class="mono">$(Esc $dedupClientSecret)</td></tr>
  <tr><td colspan="2" class="note">Not actually enforced by the local mock (any client id/secret is accepted) - recorded here purely as the value appsettings.Development.json/user-secrets should carry.</td></tr>
</table>

<h2>Cosmos DB Emulator <span class="pill">local-emulators</span> $(ReadyPill 'cosmos' $portStatus.COSMOS_ENDPOINT_PORT $cosmosReady)</h2>
<table>
  <tr><th>Setting</th><th>Value</th></tr>
  <tr><td>CosmosDb:AccountEndpoint</td><td class="mono"><a href="$(Esc $cosmosEndpoint)">$(Esc $cosmosEndpoint)</a></td></tr>
  <tr><td>CosmosDb:EmulatorKey</td><td class="mono wrap">$(Esc $cosmosKey)</td></tr>
  <tr><td>Data Explorer</td><td><a href="$(Esc $cosmosExplorerUrl)">$(Esc $cosmosExplorerUrl)</a> $(StatusPill $portStatus.COSMOS_EXPLORER_PORT)</td></tr>
</table>

<h2>Service Bus emulator + SQL Server Linux <span class="pill">local-emulators</span> $(ReadyPill 'servicebus' $portStatus.SERVICEBUS_AMQP_PORT $sbReady)</h2>
<table>
  <tr><th>Setting</th><th>Value</th></tr>
  <tr><td>ServiceBus:ConnectionString</td><td class="mono wrap">$(Esc $sbConnectionString)</td></tr>
  <tr><td>Administration Client connection string</td><td class="mono wrap">$(Esc $sbAdminConnectionString)</td></tr>
  <tr><td>SQL Server Linux SA password</td><td class="mono">$(Esc $sqlSaPassword)</td></tr>
  <tr><td colspan="2" class="note">SharedAccessKeyName/SharedAccessKey are display values only - UseDevelopmentEmulator=true bypasses real SAS validation.</td></tr>
</table>
<div class="callout">
  <strong>To browse queues/messages visually:</strong> the emulator has no built-in web UI, and
  Azure Portal can't reach it (portal browsing goes through Azure Resource Manager, which the
  emulator doesn't run) - the community <a href="https://github.com/paolosalvatori/ServiceBusExplorer">Service Bus Explorer</a>
  desktop tool is explicitly incompatible with it too, per Microsoft's own docs. Use
  <a href="https://marketplace.visualstudio.com/items?itemName=RodrigoPiccelli.servicebus-emulator-explorer">Service Bus Emulator Explorer</a>
  (VS Code extension, built specifically for the emulator) instead:
  <ol>
    <li>Install it (requires .NET 8.0 Runtime or SDK on <code>PATH</code>), then add a new connection.</li>
    <li>When prompted for the <strong>AMQP</strong> connection string, paste
      <code class="mono wrap">$(Esc $sbConnectionString)</code></li>
    <li>When prompted for the <strong>Admin</strong> connection string, paste
      <code class="mono wrap">$(Esc $sbAdminConnectionString)</code></li>
    <li>You should then see <code>inventory-events</code>, <code>inventory-state-changed</code>,
      <code>inventory-adjusted</code>, and <code>inventory-bulk-import</code> in the tree - with
      message counts, peek-without-consuming, send, and purge/delete support.</li>
  </ol>
</div>

<h2>Azurite (Blob Storage + Table Storage) accounts <span class="pill">local-emulators</span> $(StatusPill $azuriteUp)</h2>
<table>
  <tr><th>Account</th><th>Status</th><th>Key</th><th>Containers</th><th>Connection string</th></tr>
$accountRows
</table>

<div class="callout">
  <strong>Interactive web UI (rclone):</strong> <a href="$(Esc $rcloneUiUrl)/">$(Esc $rcloneUiUrl)/</a>
  $(StatusPill $portStatus.RCLONE_UI_PORT) - browse, upload, download, delete, and rename blobs from the
  browser, not just view them (unlike the static listing below). Every account above is already set up
  as its own rclone remote, named after the account. Log in with:
  <ul>
    <li>Username: <code class="mono">$(Esc $rcloneUiUsername)</code></li>
    <li>Password: <code class="mono">$(Esc $rcloneUiPassword)</code></li>
  </ul>
  Change these via <code>local-emulators\config\storage-config.json</code>'s <code>rcloneUi</code> key -
  see local-emulators\README.md's "Configuring credentials" section.
</div>

<h3>Browse containers/blobs (read-only, no login)</h3>
<p class="note">Listed directly by this script (a read+list SAS token it signs itself, valid 30 days -
re-run the script to refresh both the listing and the token) - not a live view, and Blob only (no
Table Storage). Click a container to expand it; each blob has a direct download link. Use the rclone
web UI above instead if you need to upload, delete, or rename anything.</p>
$accountBrowsers

<div class="callout">
  <strong>For Table Storage</strong>, or a live/writable view instead of this page's static listing,
  use <a href="https://azure.microsoft.com/features/storage-explorer/">Azure Storage Explorer</a>
  (free, Microsoft, Windows/Mac/Linux):
  <ol>
    <li>Install it, then in the <strong>Connect</strong> panel choose
      <strong>Add an account</strong> &rarr; <strong>Attach to a resource</strong> &rarr;
      <strong>Storage account or service</strong> &rarr; <strong>Connection string</strong>.</li>
    <li>Paste the connection string from the row above for the account you want to browse.</li>
    <li>Repeat for the other account - each row above is a separate attached account in Storage
      Explorer, not one combined view.</li>
    <li>Expand the account &rarr; <strong>Blob Containers</strong> to browse/upload/download.</li>
  </ol>
  No install? <code>podman exec -it iis-wms-emulators-tools az storage blob list --container-name
  &lt;name&gt; --connection-string "&lt;connection string above&gt;" --output table</code> lists a
  container's blobs from the terminal instead.
</div>

<h2>All host ports</h2>
<table>
  <tr><th>Port</th><th>Used by</th><th>Status</th></tr>
  <tr><td class="mono">$($ports.KAFKA_BROKER_PORT)</td><td>Kafka broker (SASL_PLAINTEXT_HOST)</td><td>$(StatusPill $portStatus.KAFKA_BROKER_PORT)</td></tr>
  <tr><td class="mono">$($ports.SCHEMA_REGISTRY_PORT)</td><td>Schema Registry</td><td>$(StatusPill $portStatus.SCHEMA_REGISTRY_PORT)</td></tr>
  <tr><td class="mono">$($ports.KAFKA_UI_PORT)</td><td>Kafka UI</td><td>$(StatusPill $portStatus.KAFKA_UI_PORT)</td></tr>
  <tr><td class="mono">$($ports.KAFKA_REST_PORT)</td><td>Kafka REST Proxy</td><td>$(StatusPill $portStatus.KAFKA_REST_PORT)</td></tr>
  <tr><td class="mono">$($ports.EVENTS_API_PORT)</td><td>events-api.ps1 wrapper</td><td>$(StatusPill $portStatus.EVENTS_API_PORT)</td></tr>
  <tr><td class="mono">$($ports.COSMOS_ENDPOINT_PORT)</td><td>Cosmos DB Emulator endpoint</td><td>$(ReadyPill 'cosmos' $portStatus.COSMOS_ENDPOINT_PORT $cosmosReady)</td></tr>
  <tr><td class="mono">$($ports.COSMOS_HEALTH_PORT)</td><td>Cosmos DB Emulator health probe</td><td>$(StatusPill $portStatus.COSMOS_HEALTH_PORT)</td></tr>
  <tr><td class="mono">$($ports.COSMOS_EXPLORER_PORT)</td><td>Cosmos DB Data Explorer</td><td>$(StatusPill $portStatus.COSMOS_EXPLORER_PORT)</td></tr>
  <tr><td class="mono">$($ports.SERVICEBUS_AMQP_PORT)</td><td>Service Bus emulator AMQP</td><td>$(ReadyPill 'servicebus' $portStatus.SERVICEBUS_AMQP_PORT $sbReady)</td></tr>
  <tr><td class="mono">$($ports.SERVICEBUS_MGMT_PORT)</td><td>Service Bus emulator management/health</td><td>$(StatusPill $portStatus.SERVICEBUS_MGMT_PORT)</td></tr>
  <tr><td class="mono">$($ports.AZURITE_BLOB_PORT)</td><td>Azurite Blob Storage</td><td>$(StatusPill $portStatus.AZURITE_BLOB_PORT)</td></tr>
  <tr><td class="mono">$($ports.AZURITE_TABLE_PORT)</td><td>Azurite Table Storage</td><td>$(StatusPill $portStatus.AZURITE_TABLE_PORT)</td></tr>
  <tr><td class="mono">$($ports.RCLONE_UI_PORT)</td><td>rclone web UI (Blob browser)</td><td>$(StatusPill $portStatus.RCLONE_UI_PORT)</td></tr>
</table>

<p class="note">See <code>scripts\local-kafka\README.md</code> and <code>scripts\local-emulators\README.md</code>'s own
"Configuring ports"/"Configuring credentials" sections to change any value above.</p>
<script>
  // The refresh (&#x21bb;) button next to each Ready/Down pill re-runs that one health check live,
  // without re-running this whole script - it hits the same readiness endpoint Test-HttpReady used
  // at generation time (Cosmos DB Emulator's /ready, Service Bus emulator's /health). This page is
  // meant to be served as http://localhost:<port> by a small dashboard-server.ps1 container (see
  // setup-podman-kafka.bat's "Opening local stack summary" step) rather than opened via file://,
  // specifically so this fetch to another http://localhost:<port> target counts as
  // private-to-private and isn't blocked outright by Chromium's Private Network Access check - if
  // you're seeing every group show "Down" regardless of the emulator's real state, you're most
  // likely looking at this file opened directly (file://) instead - e.g. this script was run
  // standalone with that container not already up - and re-running setup-podman-kafka.bat (or just
  // opening http://localhost:8098/, if that container is already running) is the fix, not anything
  // below. What this code below still can't do anything about is the ordinary CORS case: unlike
  // the PowerShell side, this can't always read the actual response status, because if the
  // emulator doesn't itself send CORS headers, the browser blocks reading the response even though
  // the request succeeded. This falls back to a no-cors probe in that case, which can only prove
  // the endpoint is reachable, not whether it returned success - reachable-but-blocked is shown as
  // "Up (not ready)" rather than "Ready", so it degrades to a safe under-statement instead of a
  // wrong "Ready".
  var READY_ENDPOINTS = {
    cosmos: "$cosmosReadyUrl",
    servicebus: "$sbHealthUrl"
  };

  function setReadyPill(group, cls, text) {
    document.querySelectorAll('.pill[data-ready-group="' + group + '"]').forEach(function (pill) {
      pill.classList.remove('up', 'down', 'warn');
      pill.classList.add(cls);
      pill.textContent = text;
    });
  }

  async function refreshReadyGroup(group, btn) {
    var url = READY_ENDPOINTS[group];
    if (!url) { return; }
    btn.disabled = true;
    btn.classList.add('spinning');
    try {
      var ready = false;
      var reachable = false;
      try {
        var res = await fetch(url, { mode: 'cors', cache: 'no-store' });
        reachable = true;
        ready = res.ok;
      } catch (corsErr) {
        try {
          await fetch(url, { mode: 'no-cors', cache: 'no-store' });
          reachable = true;
        } catch (netErr) {
          reachable = false;
        }
      }
      if (ready) {
        setReadyPill(group, 'up', 'Ready');
      } else if (reachable) {
        setReadyPill(group, 'warn', 'Up (not ready)');
      } else {
        setReadyPill(group, 'down', 'Down');
      }
    } finally {
      btn.disabled = false;
      btn.classList.remove('spinning');
    }
  }
</script>
</body>
</html>
"@

Set-Content -Path $OutFile -Value $html -Encoding utf8
Write-Host "Wrote local stack summary to $OutFile"

if (-not $NoLaunch) {
    # Prefer a dashboard-server.ps1 container already serving this exact file (started by
    # setup-podman-kafka.bat - see its "Opening local stack summary" step) - opening THAT URL
    # rather than this file directly is what makes the refresh (&#x21bb;) buttons below actually
    # work (see refreshReadyGroup()'s own comment for why). Falls back to the file directly if
    # nothing answers there (e.g. this script run standalone, without that container up) - a
    # missing/wrong dashboard container is exactly the same "never errors out just because
    # something else is missing" fallback philosophy the rest of this script already follows.
    if (Test-TcpPort 'localhost' $ServerPort -TimeoutMs 300) {
        Start-Process "http://localhost:$ServerPort/"
    } else {
        Write-Warning "No dashboard container found listening on port $ServerPort - opening $OutFile directly instead. Its refresh buttons will likely show emulators as Down regardless of their real state (Chromium blocks the health check fetches from a file:// page). Run setup-podman-kafka.bat (or setup-podman-local-stack.bat), which starts that small dashboard container automatically, to get a working refresh."
        Start-Process $OutFile
    }
}
