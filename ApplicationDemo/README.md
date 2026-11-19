# Fabric Customer 360 Demo

This folder contains a dummy, end-to-end Fabric lakehouse pipeline. It uses
synthetic data only and demonstrates parallel ingestion, Bronze/Silver/Gold
processing, and a final reconciliation step.

## Contents

- `pipelines/customer_360_pipeline.json` - orchestration graph and notebook
  parameters.
- `notebooks/` - five PySpark notebook source files called by the pipeline.
- `lakehouse/lakehouse-catalog.json` - logical table and layer catalog.

## Execution flow

1. `Ingest_Customers` and `Ingest_Transactions` run in parallel and write
   `bronze_customers` and `bronze_transactions`.
2. `Transform_Silver` deduplicates customers, validates transactions, and writes
   curated tables plus reject tables.
3. `Build_Gold` builds a customer-level summary and monthly revenue.
4. `Reconcile_Gold` checks keys and aggregate totals and writes
   `quality_reconciliation`.

## Running in Fabric

The GitHub Actions workflow builds the Lakehouse, notebooks, and DataPipeline
items and binds each notebook to the demo Lakehouse. To deploy manually instead,
create the same items in Fabric, attach the Lakehouse to each notebook, and mark
each `run_id` assignment as a notebook parameter cell.

## GitHub Actions deployment

The workflow at `.github/workflows/deploy-fabric.yml` runs on pushes to `main`
that change this folder, or manually through `workflow_dispatch`. It builds a
temporary Fabric item-source tree from the demo blueprint and publishes the
Lakehouse, notebooks, and DataPipeline using `fabric-cicd`. It does not remove
items from the workspace.

Configure these GitHub repository settings before running it:

- Variable `FABRIC_WORKSPACE_ID`: target workspace GUID.
- Secrets `AZURE_CLIENT_ID`, `AZURE_CLIENT_SECRET`, `AZURE_TENANT_ID`, and
  `AZURE_SUBSCRIPTION_ID`: service principal credentials used by `azure/login`.
- Enable service-principal access in Fabric tenant settings and grant the
  application Contributor (or higher) access to the target workspace.

The GitHub runner validates the generated item package before authenticating.
The JSON in `pipelines/` and `lakehouse/` remains a readable demo blueprint;
the deployment adapter produces the Fabric item files required by the publisher.
