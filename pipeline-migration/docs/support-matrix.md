# Support matrix

This matrix summarizes what the `pipeline-migration` skill attempts to automate, what it
passes through, and what it parks for manual follow-up.

For exact transformation rules, use the resource files listed in `SKILL.md`.

## Artifact support

| Synapse artifact | Fabric target | Support level | Notes |
|---|---|---|---|
| Pipelines | DataPipeline item | Automated | Pipeline JSON is transformed and deployed through Fabric REST APIs. |
| Activities | Fabric pipeline activities | Mixed | Supported activities are transformed or passed through with fixups. See activity support below. |
| Datasets | Inlined activity properties | Automated | Fabric has no Dataset item type for Data Factory pipelines. |
| Linked services | Fabric connections | Assisted | Existing Fabric connection display names are referenced from activity JSON. |
| Global parameters | Variable Library item | Automated | Expressions are rewritten from `globalParameters` to `libraryVariables`. |
| Synapse notebooks referenced by activities | Fabric notebooks | Prerequisite | Notebook content should be migrated before pipeline migration. |
| Triggers and schedules | Manual recreation | Not migrated | Recreate schedules manually after validating migrated pipelines. |

## Activity support

| Synapse activity | Fabric handling | Support level |
|---|---|---|
| `SynapseNotebook` | Convert to `TridentNotebook` using Fabric notebook GUID and workspace ID | Automated when target notebook exists |
| `Validation` | Rewrite as `GetMetadata` plus `IfCondition` | Automated |
| `ExecutePipeline` | Preserve child pipeline call and add required workspace context | Automated with fixups |
| `Copy` | Inline referenced datasets and map connection references | Automated for supported connectors |
| `Lookup` | Inline referenced datasets and map connection references | Automated for supported connectors |
| `GetMetadata` | Inline referenced datasets and map connection references | Automated for supported connectors |
| `SetVariable` | Preserve compatible properties and rewrite global parameter expressions | Automated |
| `AppendVariable` | Preserve compatible properties and rewrite global parameter expressions | Automated |
| `IfCondition` | Preserve compatible control flow | Pass through with expression fixups |
| `ForEach` | Preserve compatible control flow | Pass through with expression fixups |
| `Until` | Preserve compatible control flow | Pass through with expression fixups |
| `Wait` | Preserve compatible properties | Pass through |
| `WebActivity` | Preserve compatible properties | Pass through with validation recommended |
| SSIS package execution | Park for manual redesign | Parked |
| SHIR-exclusive connectors | Park for manual connector/runtime decision | Parked |
| Databricks activities | Park for manual redesign or notebook migration strategy | Parked |
| Azure Batch activities | Park for manual redesign | Parked |

## Safety defaults

| Area | Default |
|---|---|
| First recommended action | Read-only assessment |
| Existing Fabric items | Do not overwrite without explicit confirmation |
| Pipeline names | Use a suffix such as `_migrated` during migration |
| Missing notebooks | Pause and report |
| Missing connections | Pause and report |
| Triggers and schedules | Excluded from migration |

## Recommended migration order

1. Run read-only assessment.
2. Migrate referenced notebooks to Fabric.
3. Create or verify Fabric connections.
4. Convert global parameters to a Variable Library.
5. Inline datasets into activity properties.
6. Transform pipeline activities and deploy DataPipeline items.
7. Validate migrated pipelines.
8. Recreate triggers or schedules manually in Fabric.
