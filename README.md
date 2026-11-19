# HCDM-ESDF Fabric Application

This repository contains the source definitions, application metadata, environment configuration,
deployment manifest, tests, and documentation for the HCDM ESDF application.

## Ownership model

- One repository represents one application.
- One application has one Fabric workspace per environment and region.
- A workspace can contain multiple Fabric items/jobs.
- The shared CI/CD framework owns reusable deployment logic.
- This repository owns application-specific definitions and configuration.

## Environments

| Environment | Workspace |
|---|---|
| DEV | `HCDM-ESDF-DEV-WESTUS-WSP-01` |
| QA | `HCDM-ESDF-QA-WESTUS-WSP-01` |
| PERF | `HCDM-ESDF-PERF-WESTUS-WSP-01` |
| STAGE | `HCDM-ESDF-STAGE-WESTUS-WSP-01` |
| PROD | `HCDM-ESDF-PROD-WESTUS-WSP-01` |

East US production/DR is intentionally not wired into the first skeleton. Its deployment and recovery model must be agreed separately.

## Getting started

1. Add Fabric item definitions under `fabric/` using the supported source-control format for each item type.
2. Update `deployment/manifest.json` with the real item IDs, paths, types, dependencies, and deployment order.
3. Update environment files under `config/` with approved non-secret settings and workspace identifiers.
4. Add unit, integration, and data-quality tests under `tests/`.
5. Connect `azure-pipelines.yml` to the centrally managed CI/CD templates when that framework repository is available.

## Important

- Do not commit passwords, client secrets, access tokens, certificates, or connection credentials.
- Do not assume every Fabric item type is deployable through the same API/library. Validate support for each item type before enabling it in a production deployment.
- The manifest is an application inventory; the shared deployment engine will interpret it.
