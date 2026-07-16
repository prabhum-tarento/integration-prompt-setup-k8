<#
.SYNOPSIS
Prints the AZURITE_ACCOUNTS environment variable value (account1:key1;account2:key1;...) built from
the accounts declared in .\config\storage-config.json, for setup-podman-emulators.bat to pass to the
Azurite container at startup.

.DESCRIPTION
Azurite's custom-account feature - AZURITE_ACCOUNTS, format "account:key[:key2];account2:key[:key2];..."
(see https://github.com/Azure/Azurite/blob/main/README.md#customizable-storage-accounts--keys) - is
read once at container start (setup-podman-emulators.bat passes it via `-e`), before
setup-storage-data.ps1 (which provisions each account's containers/tables, and needs the Azurite
container already running) ever gets a chance to run. That ordering is why this is a separate, tiny
script rather than folded into setup-storage-data.ps1 - this one only needs to read
storage-config.json and print a value, not talk to Podman/Azurite/az at all. Plain stdout, not
JSON/an object, so the calling .bat can capture it directly via `for /f`.

**Setting AZURITE_ACCOUNTS at all disables Azurite's own default "devstoreaccount1" account** unless
that name is itself included in storage-config.json's accounts array - this repo's local dev setup
intentionally does not include it, since every account this app actually needs (the Hot/Cold tiers,
integration-resiliency.instructions.md Sec5) is declared there by its own name instead. If you add a
third account (or a third tier) later, add it to storage-config.json's accounts array - no other
wiring is needed here.

.PARAMETER ConfigDir
Folder containing storage-config.json. Defaults to .\config next to this script.

.EXAMPLE
scripts\local-emulators\get-azurite-accounts-env.ps1
#>
[CmdletBinding()]
param(
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

foreach ($account in $Accounts) {
    if (-not $account.name) { throw "$configFile has an account with no name." }
    if (-not $account.key) { throw "$configFile's account '$($account.name)' has no key." }
}

($Accounts | ForEach-Object { "$($_.name):$($_.key)" }) -join ';'
