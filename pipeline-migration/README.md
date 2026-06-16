# Pipeline Migration — Synapse Data Factory → Microsoft Fabric

An AI **skill** that migrates Azure Synapse Analytics Data Factory pipeline artifacts to
Microsoft Fabric Data Factory. Point a skill-aware AI agent (GitHub Copilot, Claude Code,
Cursor, Codex, etc.) at this folder and ask it to migrate your Synapse pipelines.

## What it does

| Source (Synapse) | Target (Fabric) |
|---|---|
| Linked services | Fabric **Connections** |
| Dataset definitions | **Inlined** into pipeline activity `typeProperties` (Fabric has no Dataset item) |
| Global parameters | **Variable Library** item (`@pipeline().libraryVariables.<name>`) |
| `SynapseNotebook` activities | `TridentNotebook` activities (referenced by notebook GUID) |
| `Validation` activity | `GetMetadata` + `IfCondition` pair |
| Compatible control-flow & data activities | Passed through with property fixups |

**Parked (manual follow-up required):** SSIS package execution, SHIR-exclusive connectors,
Databricks activities, and Azure Batch. See
[resources/pipeline-gotchas.md](resources/pipeline-gotchas.md).

> Triggers/schedules are intentionally **not** migrated — recreate them in Fabric after
> validating the migrated pipeline.

## Repository layout

```
pipeline-migration/
├── skill.json        # Skill package metadata for discovery
├── SKILL.md          # The skill instructions the AI agent reads
├── docs/             # User-facing references
├── examples/         # Quickstart and copy/paste prompts
└── resources/        # Reference docs the agent loads on demand
    ├── pipeline-assessment.md                  # Read-only scope/complexity report (run first)
    ├── pipeline-orchestrator.md                # End-to-end inline migration runner
    ├── activity-mapping.md                     # Full Synapse→Fabric activity table
    ├── notebook-activity-migration.md          # SynapseNotebook → TridentNotebook
    ├── linked-service-to-connection.md         # Linked services → Fabric Connections
    ├── dataset-inlining.md                     # Embedding datasets into activities
    ├── global-parameters-to-variable-library.md
    ├── pipeline-gotchas.md                     # Parked activities & troubleshooting
    ├── validation-testing.md                   # Post-migration verification
    └── migration-report.md                     # Migration summary generation
```

## Prerequisites

- An Azure **Synapse** workspace with the pipelines you want to migrate.
- A target Microsoft **Fabric** workspace (with capacity assigned).
- [Azure CLI](https://learn.microsoft.com/cli/azure/install-azure-cli) (`az`) signed in with
  access to both Synapse and Fabric.
- A skill-aware AI coding agent.
- **Migrate notebooks first** — `TridentNotebook` activities reference Fabric notebooks by
  GUID, so the notebooks must already exist in the Fabric workspace before the pipeline that
  calls them is migrated.

## How to use

1. Clone or download this repo so your agent can read the `pipeline-migration/` folder.
2. Sign in with the Azure CLI: `az login`.
3. Start with the [quickstart](examples/quickstart.md), then ask your agent to run the skill.
   Example prompts:

   - *"Use the pipeline-migration skill to assess my Synapse workspace `my-synapse-ws` before migrating."*
   - *"Migrate all pipelines from Synapse workspace `my-synapse-ws` to Fabric workspace `My Fabric WS`, appending `_migrated` to each name."*

For more copy/paste prompts, see [examples/prompts.md](examples/prompts.md).
For activity coverage, see [docs/support-matrix.md](docs/support-matrix.md).

You can also use the optional CLI harness to check local readiness or generate safe
agent prompts:

```bash
./pipeline-migration/bin/pipeline-migration check-package
./pipeline-migration/bin/pipeline-migration doctor
./pipeline-migration/bin/pipeline-migration assess --synapse-workspace my-synapse-ws
```

The agent reads [SKILL.md](SKILL.md), then loads only the resource files it needs. It will:

- **Assess** (optional, read-only) — query Synapse APIs and produce a scope/complexity report.
- **Discover** — auto-resolve subscription, resource group, Fabric workspace ID, notebook GUIDs, and connection names.
- **Migrate** — rewrite activities, inline datasets, convert global parameters, and deploy the pipeline to Fabric via REST.
- **Pause and report** if a referenced notebook or connection is missing in Fabric.

You provide: Synapse workspace name, Fabric workspace name, which pipelines (`*` for all), and an optional name suffix. Everything else is auto-discovered.

## Authentication / token audiences

| Target | Token audience |
|---|---|
| Synapse data plane (pipelines, datasets, linked services) | `https://dev.azuresynapse.net` |
| Synapse ARM (global parameters, workspace properties) | `https://management.azure.com` |
| Fabric REST API | `https://api.fabric.microsoft.com` |

Acquire tokens with:

```bash
az account get-access-token --resource <audience> --query accessToken -o tsv
```

## Sample: `SynapseNotebook` → `TridentNotebook`

**Before (Synapse):**

```json
{
  "name": "Run_Notebook",
  "type": "SynapseNotebook",
  "typeProperties": {
    "notebook": { "referenceName": "MyNotebook", "type": "NotebookReference" },
    "sparkPool": { "referenceName": "BigPool", "type": "BigDataPoolReference" }
  }
}
```

**After (Fabric)** — notebook referenced by GUID; `sparkPool`/`sessionConfiguration` removed
(pool/session config belongs to the Fabric Environment):

```json
{
  "name": "Run_Notebook",
  "type": "TridentNotebook",
  "typeProperties": {
    "notebookId": "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx",
    "workspaceId": "yyyyyyyy-yyyy-yyyy-yyyy-yyyyyyyyyyyy"
  }
}
```

**Global parameter expression rewrite:**

```text
Before (Synapse):  @pipeline().globalParameters.batchDate
After  (Fabric):   @pipeline().libraryVariables.batchDate
```

## Key gotchas

- Fabric has **no Dataset item type** — all dataset properties must be inlined into activities.
- Linked service references become Fabric **Connection display names** (not GUIDs) in activity JSON.
- `TridentNotebook` activities reference notebooks by **GUID**, never by name.
- The `Validation` activity type does not exist in Fabric — rewrite as `GetMetadata` + `IfCondition`.
- `ExecutePipeline` requires `workspaceId` — even for same-workspace child pipelines.

See [resources/pipeline-gotchas.md](resources/pipeline-gotchas.md) for the full troubleshooting list.

## License

See the [repository LICENSE](../LICENSE) if present. Provided as-is for the Microsoft Fabric community.
