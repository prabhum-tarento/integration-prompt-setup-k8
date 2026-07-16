<#
.SYNOPSIS
Builds an rclone config file (INI format) with one remote per account in .\config\storage-config.json,
for the rclone/rclone web GUI container (https://rclone.org/docker/,
https://rclone.org/commands/rclone_rcd/#web-gui) setup-podman-emulators.bat starts alongside Azurite -
a real interactive browser UI (browse/upload/download/delete/rename) rather than the read-only,
regenerate-to-refresh listing generate-local-stack-summary.ps1's own "Browse containers/blobs" section
already provides (see that script's header comment for why that one exists: ghcr.io/adrianhall/
azurite-ui, the more purpose-built option, turned out not to be publicly pullable).

.DESCRIPTION
Each remote is named after its account (matching storage-config.json's accounts[].name) and points at
Azurite by container name, not "localhost" - this file is mounted into the rclone-ui container itself, a
different network namespace than the host, same reasoning setup-storage-data.ps1's own header comment
gives for using AzuriteContainer instead of "localhost" there. `use_emulator = true` plus explicit
account/key/endpoint is deliberate - it tells rclone's azureblob backend it's talking to an emulator
(relaxing some real-Azure-only assumptions) while still using this account's own name/key/endpoint
rather than Azurite's built-in devstoreaccount1 (which this repo's setup doesn't use - see
get-azurite-accounts-env.ps1's own header comment).

Safe to re-run: always overwrites -OutFile from the current storage-config.json, so editing that file
(adding an account, rotating a key) and re-running this (or the whole setup-podman-emulators.bat) picks
up the change - the rclone-ui container itself is always removed and recreated fresh right after, same
as every other container in that script.

.PARAMETER OutFile
Where to write the generated rclone.conf. Not committed to the repo (contains each account's storage
key) - same "local, throwaway value, but still not committed" reasoning as generate-local-stack-
summary.ps1's own -OutFile default.

.PARAMETER AzuriteContainer
Name of the Azurite container on the same Podman network - the rclone-ui container reaches it by this
name, not "localhost" (see .DESCRIPTION). Matches setup-podman-emulators.bat's AZURITE_CONTAINER.

.PARAMETER ConfigDir
Folder containing storage-config.json. Defaults to .\config next to this script.

.EXAMPLE
scripts\local-emulators\generate-rclone-config.ps1 -OutFile $env:TEMP\iis-wms-rclone.conf
#>
[CmdletBinding()]
param(
    [Parameter(Mandatory)]
    [string]$OutFile,
    [string]$AzuriteContainer = 'iis-wms-storage-emulator',
    [string]$ConfigDir
)

$ErrorActionPreference = 'Stop'

# See setup-cosmos-data.ps1's own header note on why this defaults in the body, not in param() - same
# [CmdletBinding()]/$PSScriptRoot-in-param() quirk, same nested-inside-a-.bat invocation here.
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

$lines = @()
foreach ($account in $Accounts) {
    if (-not $account.name) { throw "$configFile has an account with no name." }
    if (-not $account.key) { throw "$configFile's account '$($account.name)' has no key." }
    $lines += "[$($account.name)]"
    $lines += 'type = azureblob'
    $lines += "account = $($account.name)"
    $lines += "key = $($account.key)"
    $lines += "endpoint = http://${AzuriteContainer}:10000/$($account.name)"
    $lines += 'use_emulator = true'
    $lines += ''
}

Set-Content -Path $OutFile -Value ($lines -join "`n") -Encoding utf8
Write-Host "Wrote rclone config ($($Accounts.Count) remote(s): $((($Accounts | ForEach-Object { $_.name }) -join ', '))) to $OutFile"
