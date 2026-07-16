<#
.SYNOPSIS
Creates the Blob containers (and, if any are declared, Table Storage tables) for every account listed
in .\config\storage-config.json against the Azurite emulator started by setup-podman-emulators.bat -
one pass per account, since each is a genuinely separate Storage account on that one Azurite instance
(see get-azurite-accounts-env.ps1's own header comment on Azurite's AZURITE_ACCOUNTS feature).

.DESCRIPTION
Unlike setup-cosmos-data.ps1 (which signs Cosmos REST calls directly from this process), this script
shells out to the Azure CLI already installed in the iis-wms-emulators-tools container
(image/tools.Containerfile) via "podman exec" - see that container's own README.md section for why it
exists. `az storage container create`/`az storage table create` are data-plane calls straight to the
storage account's REST endpoint (unlike `az cosmosdb`/`az servicebus`, which talk to Azure Resource
Manager and have nothing to provision against either emulator here - see README.md's "What this
container is not"), so this is exactly the kind of ad-hoc-from-the-same-network use that container was
built for, just scripted instead of typed by hand. This keeps the well-known Azure.Storage.Blobs/
Azure.Data.Tables SDKs out of this script (CLAUDE.md's "never introduce a new package without
approval" rule) without hand-rolling the Shared Key / Shared Key Lite request-signing algorithms the
way setup-cosmos-data.ps1 hand-rolls Cosmos's.

This process itself has no route to the Azurite container by container name (only setup-podman-
emulators.bat's own published localhost ports), so every az invocation here targets Azurite via the
Podman network (http://<AzuriteContainer>:10000/10002/<account name>) from inside the tools container,
using each account's own name/key from storage-config.json - not the localhost form the app itself
uses (see appsettings.Development.json's BlobStorage:Hot/Cold:AccountUri), which only resolves
correctly from the host.

Safe to re-run: both `az storage container create` and `az storage table create` succeed (exit 0)
whether or not the resource already existed - re-running this after editing storage-config.json adds
whatever's new (including a whole new account) without erroring on what's already there.

.PARAMETER ToolsContainer
Name of the Azure CLI tools container (image/tools.Containerfile) this script execs into - matches
setup-podman-emulators.bat's TOOLS_CONTAINER.

.PARAMETER AzuriteContainer
Name of the Azurite container on the same Podman network - matches setup-podman-emulators.bat's
AZURITE_CONTAINER. Used (not "localhost") because every command here actually runs inside
ToolsContainer via podman exec, a different network namespace than this script's own.

.PARAMETER AzuriteBlobPort
Displayed only (in this script's own final summary, as part of the host-reachable connection string
form) - does not affect where this script itself sends its az calls, which always use Azurite's fixed
container-internal port (10000) over the Podman network regardless of how this has been remapped on
the host. Defaults to 10000 (Azurite's own default), matching setup-podman-emulators.bat's
AZURITE_BLOB_PORT.

.PARAMETER AzuriteTablePort
Same as AzuriteBlobPort, for the Table endpoint (container-internal 10002). Defaults to 10002.

.PARAMETER ConfigDir
Folder containing storage-config.json (an object with an accounts array; each entry a
{name, key, containers, tables} object - containers/tables may be empty). Defaults to .\config next to
this script.

.EXAMPLE
scripts\local-emulators\setup-storage-data.ps1
#>
[CmdletBinding()]
param(
    [string]$ToolsContainer = 'iis-wms-emulators-tools',
    [string]$AzuriteContainer = 'iis-wms-storage-emulator',
    [string]$AzuriteBlobPort = '10000',
    [string]$AzuriteTablePort = '10002',
    [string]$ConfigDir
)

$ErrorActionPreference = 'Stop'

# See setup-cosmos-data.ps1's own header note on why this defaults in the body, not in param() -
# same [CmdletBinding()]/$PSScriptRoot-in-param() quirk, same nested-inside-a-.bat invocation here.
if (-not $ConfigDir) { $ConfigDir = Join-Path $PSScriptRoot 'config' }

$configFile = Join-Path $ConfigDir 'storage-config.json'
if (-not (Test-Path $configFile)) {
    throw "Config file not found at $configFile - expected an object with an accounts array."
}
$storageConfig = Get-Content -Raw -Path $configFile | ConvertFrom-Json

# ConvertFrom-Json hands back a bare scalar for a single-element JSON array - coerce to a real array,
# same reasoning as setup-cosmos-data.ps1's $Containers handling.
$Accounts = $storageConfig.accounts
if ($null -eq $Accounts) { $Accounts = @() }
elseif ($Accounts -isnot [array]) { $Accounts = @($Accounts) }

if ($Accounts.Count -eq 0) {
    throw "$configFile has no accounts - expected a non-empty array of {name, key, containers, tables} objects."
}

function Invoke-ToolsAz {
    param([string[]]$AzArgs, [string]$ConnectionString)

    & podman exec $ToolsContainer az @AzArgs --connection-string $ConnectionString --output none
    if ($LASTEXITCODE -ne 0) {
        throw "az $($AzArgs -join ' ') failed (exit $LASTEXITCODE) - see output above."
    }
}

$summaries = @()

foreach ($account in $Accounts) {
    $name = $account.name
    $key = $account.key
    if (-not $name) { throw "$configFile has an account with no name." }
    if (-not $key) { throw "$configFile's account '$name' has no key." }

    # ConvertFrom-Json's single-element-array-becomes-a-scalar quirk applies per account too - see
    # setup-cosmos-data.ps1's own header note.
    $containers = $account.containers
    if ($null -eq $containers) { $containers = @() }
    elseif ($containers -isnot [array]) { $containers = @($containers) }

    $tables = $account.tables
    if ($null -eq $tables) { $tables = @() }
    elseif ($tables -isnot [array]) { $tables = @($tables) }

    $networkConnectionString = "DefaultEndpointsProtocol=http;AccountName=$name;AccountKey=$key;" +
        "BlobEndpoint=http://${AzuriteContainer}:10000/${name};" +
        "TableEndpoint=http://${AzuriteContainer}:10002/${name};"

    Write-Host ''
    Write-Host "=== Account '$name' - Blob containers ==="
    if ($containers.Count -eq 0) {
        Write-Host 'No containers declared for this account - skipping.'
    } else {
        foreach ($container in $containers) {
            Write-Host "Creating container '$container'..."
            Invoke-ToolsAz -AzArgs @('storage', 'container', 'create', '--name', $container) -ConnectionString $networkConnectionString
        }
    }

    Write-Host ''
    Write-Host "=== Account '$name' - Tables ==="
    if ($tables.Count -eq 0) {
        Write-Host 'No tables declared for this account - nothing in this codebase uses Table Storage yet (see README.md); skipping.'
    } else {
        foreach ($table in $tables) {
            Write-Host "Creating table '$table'..."
            Invoke-ToolsAz -AzArgs @('storage', 'table', 'create', '--name', $table) -ConnectionString $networkConnectionString
        }
    }

    $localConnectionString = "DefaultEndpointsProtocol=http;AccountName=$name;AccountKey=$key;" +
        "BlobEndpoint=http://localhost:${AzuriteBlobPort}/${name};" +
        "TableEndpoint=http://localhost:${AzuriteTablePort}/${name};"
    $summaries += [pscustomobject]@{
        Name       = $name
        Containers = if ($containers.Count -eq 0) { '(none)' } else { $containers -join ', ' }
        Tables     = if ($tables.Count -eq 0) { '(none)' } else { $tables -join ', ' }
        LocalConn  = $localConnectionString
    }
}

Write-Host ''
Write-Host '=== Done - account connection strings for appsettings.Development.json ==='
foreach ($s in $summaries) {
    Write-Host ''
    Write-Host "Account:    $($s.Name)"
    Write-Host "Containers: $($s.Containers)"
    Write-Host "Tables:     $($s.Tables)"
    Write-Host "AccountUri: $($s.LocalConn)"
}
