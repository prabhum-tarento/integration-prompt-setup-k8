@echo off
REM Runs the two local Podman setups this repo has - scripts\local-emulators (Cosmos DB Emulator +
REM Service Bus emulator + Azure CLI tools image) and scripts\local-kafka (Kafka broker + Schema
REM Registry + Kafka UI + Kafka REST Proxy) - one after the other, as a single entry point, so you
REM don't have to remember to run both separately before starting the app locally. This is a thin
REM wrapper - all the actual container/network logic still lives in each sub-script; see their own
REM README.md (local-emulators\README.md, local-kafka\README.md) for what each one starts and why.
REM
REM Each sub-script runs on its own dedicated Podman network (iis-wms-emulators-net,
REM iis-wms-kafka-net) - they're independent stacks, not bridged together, since the app under test
REM runs directly on the host either way and reaches every container via its published localhost
REM port, not by container name. Nothing here changes that.
REM
REM Usage: setup-podman-local-stack.bat [WarehouseId] [Sku] [Quantity] [EventType] [ConsumerGroup]
REM
REM EULA acceptance for the Service Bus emulator/SQL Server Linux dependency (forwarded to
REM setup-podman-emulators.bat, whose own containers won't start without it - see that script's
REM header comment and local-emulators\README.md's "License terms you're accepting" section for
REM the two EULA links) defaults to ACCEPTED here - see the ACCEPT_EULA_FLAG note just below the
REM EULA text further down for why. The five positional arguments are all optional and are
REM forwarded as-is to setup-podman-kafka.bat (see local-kafka\README.md's "Quick start" for their
REM meaning/defaults) - this script itself doesn't interpret them.
REM
REM setup-podman-kafka.bat's last step runs registration\events-api.ps1 in the FOREGROUND (this
REM console becomes the one it logs each POST /api/events request to) and only returns on Ctrl+C or
REM its own /api/shutdown endpoint - so this script blocks there too, the same as running
REM setup-podman-kafka.bat directly. That's deliberate: the emulators/tools image above have
REM already finished starting by the time control reaches that point, so Ctrl+C-ing out of it
REM doesn't leave anything half-started.
REM
REM Right before that final blocking step, setup-podman-kafka.bat itself opens a one-page HTML
REM summary of every port/credential across BOTH stacks (this script's emulators above and
REM Kafka) in your default browser - see local-kafka\setup-podman-kafka.bat's own "Opening local
REM stack summary" step and generate-local-stack-summary.ps1. It's triggered from there (not
REM here) specifically so it still opens when someone runs setup-podman-kafka.bat directly,
REM without going through this combined script at all.
REM
REM Tear down each stack independently - see local-emulators\README.md and local-kafka\README.md's
REM own "Tear down"/"Cleanup" sections; there's no combined teardown here since the two stacks don't
REM share any network/volume this script would need to clean up together.

setlocal

REM Defaults to accepted (1), not 0 - a deliberate exception to the "never set a EULA silently"
REM pattern every other script in this repo follows (setup-podman-emulators.bat's own separate
REM gate, called below, still defaults to 0 and is unaffected by this). Flipped intentionally for
REM local dev convenience on this machine/repo - do not "fix" this back to 0 without checking
REM with whoever owns this decision first. -AcceptEula/ACCEPT_EULA=Y still work if ever needed
REM again (e.g. after reverting this default).
set ACCEPT_EULA_FLAG=1
if /i "%~1"=="-AcceptEula" set ACCEPT_EULA_FLAG=1
if /i "%ACCEPT_EULA%"=="Y" set ACCEPT_EULA_FLAG=1

if %ACCEPT_EULA_FLAG% neq 1 (
    echo(
    echo This starts the Azure Service Bus emulator and its SQL Server Linux dependency
    echo ^(via scripts\local-emulators\setup-podman-emulators.bat^), both of which require
    echo accepting their own license terms before they'll run:
    echo(
    echo   Service Bus emulator EULA: https://github.com/Azure/azure-service-bus-emulator-installer/blob/main/EMULATOR_EULA.txt
    echo   SQL Server Linux EULA:     https://go.microsoft.com/fwlink/?LinkId=746388
    echo(
    echo Re-run as "setup-podman-local-stack.bat -AcceptEula" once you've read and agree to both -
    echo or set ACCEPT_EULA=Y in your environment first. Nothing has been started.
    exit /b 1
)

echo(
echo ============================================================
echo  1/2 - Cosmos DB Emulator + Service Bus emulator + Azure CLI
echo ============================================================
call "%~dp0local-emulators\setup-podman-emulators.bat" -AcceptEula
if errorlevel 1 (
    echo(
    echo local-emulators setup failed - not starting Kafka. See the output above.
    exit /b 1
)

echo(
echo ============================================================
echo  2/2 - Kafka broker + Schema Registry + Kafka UI + REST Proxy
echo ============================================================
REM %1-%5 (WarehouseId/Sku/Quantity/EventType/ConsumerGroup), not %2-%6 - -AcceptEula used to
REM occupy %1, requiring every other argument to shift up by one; now that it's no longer
REM required (see ACCEPT_EULA_FLAG note above), %1 is free for the first real positional
REM argument again. If you're used to still typing -AcceptEula first out of habit, drop it -
REM it would otherwise be forwarded here as WarehouseId by mistake.
call "%~dp0local-kafka\setup-podman-kafka.bat" %1 %2 %3 %4 %5
if errorlevel 1 (
    echo(
    echo local-kafka setup failed. See the output above.
    exit /b 1
)

endlocal
