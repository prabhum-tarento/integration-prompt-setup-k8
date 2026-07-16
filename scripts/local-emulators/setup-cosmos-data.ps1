<#
.SYNOPSIS
Creates the database and containers declared in .\config\cosmos-db-config.json on the Cosmos DB
Emulator started by setup-podman-emulators.bat, then seeds each container from the matching JSON
file in .\data.

.DESCRIPTION
Talks directly to the Cosmos DB REST API (signed with the well-known emulator master key) rather
than the Microsoft.Azure.Cosmos SDK, so this stays a standalone script with no new project/NuGet
dependency (CLAUDE.md's "never introduce a new package without approval" rule) - see
https://learn.microsoft.com/rest/api/cosmos-db/access-control-on-cosmosdb-resources for the
request-signing algorithm this implements.

This is local-emulator provisioning, not application code - it does not conflict with
cosmos-db.instructions.md Sec2's "never call CreateDatabaseIfNotExistsAsync/
CreateContainerIfNotExistsAsync at application startup" rule, which is about what the app's own
CosmosClient does at runtime. Database/container creation is normally a Bicep/Terraform concern in
real environments (same section); this script is the local-only equivalent, the same role
config/service-bus-config.json already plays for the Service Bus emulator next to it.

Every container shares one partition key path, declared as "partitionKey" in cosmos-db-config.json
(default "/category", matching appsettings.Development.json's CosmosDb:PartitionKeyPath) - a
deliberate choice for this script, made knowing it only reflects real document shape for
OrderArchive (whose OrderArchiveDocument has a Category property). InventoryEventDocument and
InventoryBulkImportItemDocument have no category property at all - cosmos-db.instructions.md Sec4
documents their actual partitioning as the composite WarehouseId:Sku on a PartitionKey property, not
Category - so seeded InventoryEvents/BulkInventoryImports documents land in Cosmos's "Undefined"
partition under this default rather than being distributed by WarehouseId:Sku. That's a known,
accepted divergence from Sec4 for this local seed data, not a bug - change cosmos-db-config.json's
partitionKey (or pass -PartitionKey) if you need those two containers' seed data to actually
exercise WarehouseId:Sku partitioning.

Safe to re-run: database/container creation tolerates an existing resource (409), and every
document upserts (x-ms-documentdb-is-upsert), so re-running after editing a .\data\*.json file
updates that file's documents in place instead of erroring on duplicates.

.PARAMETER Endpoint
Cosmos DB Emulator endpoint - matches CosmosDb:AccountEndpoint in appsettings.Development.json.

.PARAMETER Key
Cosmos DB Emulator master key - matches CosmosDb:EmulatorKey in user-secrets (never
appsettings.Development.json - see cosmos-db.instructions.md §1). Defaults
to the emulatorKey declared in cosmos-db-config.json (see ConfigDir below), same precedence as
DatabaseName/PartitionKey below (explicit param wins, then the config file, then this script's own
built-in fallback if the config file has no emulatorKey at all) - edit that file to change the key
this script signs requests with, without touching this script. Never a production key; the built-in
fallback is the same publicly documented well-known value on every machine that runs this emulator
image, safe to keep inline.

.PARAMETER DatabaseName
Overrides the databaseName declared in cosmos-db-config.json (see ConfigDir below). Defaults to
that file's value, which itself matches CosmosDb:DatabaseName in appsettings.Development.json - leave
unset unless you specifically want to target a different database than the config file declares.

.PARAMETER DataDir
Folder containing one JSON file per container - file name (without extension) is the container name,
content is a JSON array of documents to upsert. Defaults to .\data next to this script.

.PARAMETER ConfigDir
Folder containing cosmos-db-config.json (an object with a databaseName string, a partitionKey path
string, an emulatorKey string, and a containers array of container names to create/seed). Defaults to
.\config next to this script.

.PARAMETER PartitionKey
Overrides the partitionKey path declared in cosmos-db-config.json - applied to every container.
Defaults to that file's value ("/category").

.PARAMETER DataExplorerUrl
Display-only URL for the Data Explorer, printed in this script's final "Done" message. Defaults to
the Cosmos DB Emulator's default port (http://localhost:1234) - setup-podman-emulators.bat passes
the actual configured value (config/ports.env's COSMOS_EXPLORER_PORT) so this stays accurate if
that port has been remapped to avoid a conflict.

.EXAMPLE
scripts\local-emulators\setup-cosmos-data.ps1
#>
[CmdletBinding()]
param(
    [string]$Endpoint = 'https://localhost:8081',
    [string]$Key,
    [string]$DatabaseName,
    [string]$DataDir,
    [string]$ConfigDir,
    [string]$PartitionKey,
    [string]$DataExplorerUrl = 'http://localhost:1234'
)

$ErrorActionPreference = 'Stop'

# $PSScriptRoot reads back empty if referenced directly in a param() default value on a script that
# declares [CmdletBinding()] (reproduced against Windows PowerShell 5.1 - a parameter-binding-order
# quirk, not specific to how this script is invoked) - so DataDir/ConfigDir default here in the body
# instead, where $PSScriptRoot is reliably populated regardless of invocation method (-File, &, dot-
# sourced, or nested inside another PowerShell process, as setup-podman-emulators.bat does).
if (-not $DataDir) { $DataDir = Join-Path $PSScriptRoot 'data' }
if (-not $ConfigDir) { $ConfigDir = Join-Path $PSScriptRoot 'config' }

# Database name, partition key path, and container names to create/seed come from
# config/cosmos-db-config.json rather than being hardcoded here - edit that file to rename the
# database, repartition, or add/remove a container (pairing a new one with a matching
# data\<name>.json), without touching this script. Container names must match the ContainerName
# consts in src/Infrastructure/.../Persistence/CosmosDb/Repository/*.cs exactly.
$configFile = Join-Path $ConfigDir 'cosmos-db-config.json'
if (-not (Test-Path $configFile)) {
    throw "Config file not found at $configFile - expected an object with databaseName, partitionKey, emulatorKey, and containers."
}
$cosmosConfig = Get-Content -Raw -Path $configFile | ConvertFrom-Json

if (-not $DatabaseName) { $DatabaseName = $cosmosConfig.databaseName }
if (-not $DatabaseName) {
    throw "$configFile has no databaseName - expected a non-empty string."
}

# Same explicit-param > config-file > built-in-fallback precedence as DatabaseName/PartitionKey - the
# fallback only matters for a config file predating emulatorKey, or a bare invocation with no config
# file wired up at all; every other case configures the key via cosmos-db-config.json instead.
if (-not $Key) { $Key = $cosmosConfig.emulatorKey }
if (-not $Key) { $Key = 'C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==' }

if (-not $PartitionKey) { $PartitionKey = $cosmosConfig.partitionKey }
if (-not $PartitionKey) { $PartitionKey = '/category' }
if (-not $PartitionKey.StartsWith('/')) { $PartitionKey = "/$PartitionKey" }
# The document property Cosmos extracts at $PartitionKey - e.g. "/category" -> "category" - used
# below to read each seed document's actual value at that path (single-level paths only; this
# script doesn't support a nested partition key path like "/a/b").
$PartitionKeyProperty = $PartitionKey.TrimStart('/')

$Containers = $cosmosConfig.containers
# ConvertFrom-Json already returns a multi-element JSON array as a PowerShell array - wrapping that
# again with @() would nest it as a single-element array-of-an-array. Only a single-element JSON
# array needs this coercion, since ConvertFrom-Json otherwise hands back a bare scalar for it.
if ($Containers -isnot [array]) { $Containers = @($Containers) }
if ($Containers.Count -eq 0) {
    throw "$configFile has no containers - expected a non-empty array of container names."
}

# The emulator's HTTPS endpoint uses a self-signed certificate - bypass validation for this script's
# calls only (does not change any machine-wide/process-wide trust setting beyond this process).
[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12
# Windows PowerShell's HttpWebRequest-backed Invoke-RestMethod sends an "Expect: 100-continue" header
# by default, which the Cosmos DB Emulator's HTTPS listener resets the connection on - a documented
# emulator quirk, not specific to this script. Disabling it here (this process only) is required for
# every POST below to succeed.
[Net.ServicePointManager]::Expect100Continue = $false
$IsCore = $PSVersionTable.PSEdition -eq 'Core'
if ($IsCore) {
    $script:CertParams = @{ SkipCertificateCheck = $true }
} else {
    $script:CertParams = @{}
    [Net.ServicePointManager]::ServerCertificateValidationCallback = { $true }
}

# Cosmos REST API request-signing algorithm (Microsoft Learn "Access control on Cosmos DB
# resources"): HMAC-SHA256 over "{verb}\n{resourceType}\n{resourceLink}\n{date}\n\n", keyed by the
# base64-decoded master key, then base64-encoded and URL-escaped into the Authorization header.
function New-CosmosAuthHeader {
    param([string]$Verb, [string]$ResourceType, [string]$ResourceLink, [string]$Date, [string]$MasterKey)

    $keyBytes = [Convert]::FromBase64String($MasterKey)
    $stringToSign = "$($Verb.ToLowerInvariant())`n$($ResourceType.ToLowerInvariant())`n$ResourceLink`n$($Date.ToLowerInvariant())`n`n"
    $hmac = [System.Security.Cryptography.HMACSHA256]::new($keyBytes)
    try {
        $hash = $hmac.ComputeHash([System.Text.Encoding]::UTF8.GetBytes($stringToSign))
    } finally {
        $hmac.Dispose()
    }
    $signature = [Convert]::ToBase64String($hash)
    return [Uri]::EscapeDataString("type=master&ver=1.0&sig=$signature")
}

function Invoke-CosmosRequest {
    param(
        [string]$Verb,
        [string]$ResourceType,
        [string]$ResourceLink,
        [string]$Uri,
        [string]$Body = $null,
        [hashtable]$ExtraHeaders = @{}
    )

    # RFC1123 "R" format, as the Cosmos REST API requires for both x-ms-date and the string-to-sign.
    $date = [DateTime]::UtcNow.ToString('R', [Globalization.CultureInfo]::InvariantCulture)
    $auth = New-CosmosAuthHeader -Verb $Verb -ResourceType $ResourceType -ResourceLink $ResourceLink -Date $date -MasterKey $Key

    $headers = @{
        'x-ms-date'    = $date
        'x-ms-version' = '2018-12-31'
        'Authorization' = $auth
    }
    foreach ($k in $ExtraHeaders.Keys) { $headers[$k] = $ExtraHeaders[$k] }

    $params = @{
        Method      = $Verb
        Uri         = $Uri
        Headers     = $headers
        ContentType = 'application/json'
    }
    if ($Body) { $params['Body'] = [System.Text.Encoding]::UTF8.GetBytes($Body) }

    Invoke-RestMethod @params @script:CertParams
}

function Test-Conflict {
    param($ErrorRecord)
    $response = $ErrorRecord.Exception.Response
    if ($null -eq $response) { return $false }
    return [int]$response.StatusCode -eq 409
}

Write-Host ''
Write-Host "=== Creating database '$DatabaseName' ==="
try {
    Invoke-CosmosRequest -Verb 'POST' -ResourceType 'dbs' -ResourceLink '' `
        -Uri "$Endpoint/dbs" -Body (@{ id = $DatabaseName } | ConvertTo-Json -Compress) | Out-Null
    Write-Host "Created '$DatabaseName'."
} catch {
    if (Test-Conflict $_) { Write-Host "'$DatabaseName' already exists - continuing." }
    else { throw }
}

foreach ($container in $Containers) {
    Write-Host ''
    Write-Host "=== Creating container '$container' (partition key $PartitionKey) ==="
    $body = @{
        id           = $container
        partitionKey = @{ paths = @($PartitionKey); kind = 'Hash'; version = 2 }
    } | ConvertTo-Json -Compress -Depth 5

    try {
        Invoke-CosmosRequest -Verb 'POST' -ResourceType 'colls' -ResourceLink "dbs/$DatabaseName" `
            -Uri "$Endpoint/dbs/$DatabaseName/colls" -Body $body | Out-Null
        Write-Host "Created '$container'."
    } catch {
        if (Test-Conflict $_) { Write-Host "'$container' already exists - continuing." }
        else { throw }
    }
}

foreach ($container in $Containers) {
    $dataFile = Join-Path $DataDir "$container.json"
    Write-Host ''
    Write-Host "=== Seeding '$container' from $dataFile ==="
    if (-not (Test-Path $dataFile)) {
        Write-Host "No data file for '$container' - skipping seed."
        continue
    }

    $documents = Get-Content -Raw -Path $dataFile | ConvertFrom-Json
    $count = 0
    foreach ($doc in $documents) {
        $docJson = $doc | ConvertTo-Json -Compress -Depth 10

        # The partition key header must reflect whatever value (if any) actually sits at
        # $PartitionKey in this document - not assume every document has one. A document with no
        # value there (e.g. InventoryEvents/BulkInventoryImports documents under the default
        # "/category" path - see this script's header comment) is written under Cosmos's "Undefined"
        # partition, represented on the wire as "[{}]", rather than a real partition key value.
        $partitionKeyValue = $doc.$PartitionKeyProperty
        if ($null -eq $partitionKeyValue) {
            $partitionKeyHeader = '[{}]'
        } else {
            # -InputObject (not the pipeline) is required here: piping a single-element array into
            # ConvertTo-Json unwraps it to a bare scalar instead of a one-element JSON array, which
            # the emulator then rejects as a partition-key mismatch against the document body.
            $partitionKeyHeader = ConvertTo-Json -InputObject @($partitionKeyValue) -Compress
        }

        Invoke-CosmosRequest -Verb 'POST' -ResourceType 'docs' -ResourceLink "dbs/$DatabaseName/colls/$container" `
            -Uri "$Endpoint/dbs/$DatabaseName/colls/$container/docs" -Body $docJson `
            -ExtraHeaders @{
                'x-ms-documentdb-partitionkey' = $partitionKeyHeader
                'x-ms-documentdb-is-upsert'    = 'true'
            } | Out-Null
        $count++
    }
    Write-Host "Upserted $count document(s) into '$container'."
}

Write-Host ''
Write-Host '=== Done ==='
Write-Host "Data Explorer: $DataExplorerUrl"
