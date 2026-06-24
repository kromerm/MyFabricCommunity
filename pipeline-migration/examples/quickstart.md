# Quickstart

Use this quickstart when you want an agent to assess or migrate Synapse Data Factory
pipelines to Microsoft Fabric Data Factory with the `pipeline-migration` skill.

## 1. Get the skill locally

Clone this repository or download the `pipeline-migration/` folder so your agent can read:

- `pipeline-migration/SKILL.md`
- `pipeline-migration/resources/`
- `pipeline-migration/docs/`
- `pipeline-migration/examples/`

## 2. Sign in with Azure CLI

```bash
az login
az account show
```

The skill uses Azure CLI tokens for three audiences:

| Target | Token audience |
|---|---|
| Synapse data plane | `https://dev.azuresynapse.net` |
| Synapse ARM | `https://management.azure.com` |
| Fabric REST API | `https://api.fabric.microsoft.com` |

## 3. Start with a read-only assessment

Ask your agent:

```text
Use the pipeline-migration skill in this folder to assess Synapse workspace
<synapse-workspace-name>. Do not create or update Fabric items. Print the assessment
summary and list blockers before proposing a migration plan.
```

The assessment should identify pipeline count, activity types, linked services, datasets,
global parameters, notebooks, triggers, and unsupported or parked activities.

## 4. Prepare Fabric prerequisites

Before migration, confirm:

- The target Fabric workspace exists and has capacity assigned.
- Referenced notebooks have already been migrated to Fabric.
- Required Fabric connections exist and have display names that can replace Synapse
  linked service references.
- You understand triggers and schedules are not migrated by this skill.

## 5. Run migration with a safe suffix

Ask your agent:

```text
Use the pipeline-migration skill to migrate pipelines from Synapse workspace
<synapse-workspace-name> to Fabric workspace <fabric-workspace-name>. Migrate
<pipeline-name-or-*> and append _migrated to each created Fabric pipeline name.
Pause before any destructive or overwrite action.
```

## 6. Validate before recreating schedules

After migration, run the validation flow in `resources/validation-testing.md`, then
recreate triggers or schedules manually in Fabric.
