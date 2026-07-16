# Azure CLI, pinned to a deterministic tag per Microsoft's own current guidance (learn.microsoft.com
# "How to run Azure CLI in a Docker Container", checked 2026-07) - <version>-azurelinux3.0, not
# "latest"/"azurelinux3.0" alone, so a rebuild doesn't silently pick up a newer az CLI. The Alpine-based
# tag this image used to ship as is no longer updated by Microsoft (final supported version 2.63.0,
# August 2024) - Azure Linux 3.0 is the maintained base now.
#
# This is a general-purpose ad-hoc admin container on this stack's Podman network
# (iis-wms-emulators-net) - e.g. `az login` against a real Azure subscription to compare a setting,
# or just having `curl`/`az` available in the same network namespace as the emulators below. It is
# NOT how the Cosmos DB Emulator/Service Bus emulator containers in this folder get their
# databases/containers/queues provisioned: `az cosmosdb`/`az servicebus` talk to real Azure Resource
# Manager, and neither emulator runs an ARM control plane locally - there is nothing for those `az`
# subcommands to reach here. The Cosmos DB Emulator is provisioned via the app's own CosmosClient
# calls (or the Data Explorer at http://localhost:1234) and the Service Bus emulator via
# config/service-bus-config.json (see README.md) - not via this image.
#
# Deliberately no extra RUN layers - the base image already ships `az`, Python, and enough of a
# shell for ad-hoc use. Add packages here only if you need something the base doesn't already have.
FROM mcr.microsoft.com/azure-cli:2.88.0-azurelinux3.0

WORKDIR /work
