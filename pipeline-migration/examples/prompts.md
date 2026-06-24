# Prompt examples

These prompts are intended for skill-aware agents that can read this folder and follow
`SKILL.md`.

## Assessment only

```text
Use the pipeline-migration skill in this folder to assess Synapse workspace
<synapse-workspace-name>. This is read-only: do not create, update, or delete Fabric
items. Return an executive summary, migration complexity, parked activities, missing
Fabric prerequisites, and recommended next steps.
```

## Migration plan

```text
Use the pipeline-migration skill to produce a migration plan for Synapse workspace
<synapse-workspace-name> into Fabric workspace <fabric-workspace-name>. Start with
assessment, then list the migration phases in order. Highlight notebook, connection,
dataset, global parameter, trigger, and unsupported activity work.
```

## Migrate selected pipelines

```text
Use the pipeline-migration skill to migrate these Synapse pipelines:
<pipeline-1>, <pipeline-2>, <pipeline-3>.

Source Synapse workspace: <synapse-workspace-name>
Target Fabric workspace: <fabric-workspace-name>
Fabric name suffix: _migrated

Pause and report if a referenced notebook or Fabric connection is missing. Do not
overwrite existing Fabric items unless I explicitly confirm.
```

## Migrate all pipelines

```text
Use the pipeline-migration skill to migrate all pipelines from Synapse workspace
<synapse-workspace-name> to Fabric workspace <fabric-workspace-name>, appending
_migrated to each pipeline name. Run assessment first, then pause with the summary
before creating Fabric items.
```

## Notebook activity conversion focus

```text
Use the pipeline-migration skill to inspect SynapseNotebook activities in
<synapse-workspace-name>. Map each referenced Synapse notebook name to the Fabric
notebook GUID in workspace <fabric-workspace-name>. Report any missing notebooks before
attempting pipeline migration.
```

## Linked service and dataset focus

```text
Use the pipeline-migration skill to inspect linked services and datasets used by
pipelines in <synapse-workspace-name>. Identify the Fabric connection display names
required for migration and explain which dataset properties will be inlined into
pipeline activity typeProperties.
```

## Post-migration validation

```text
Use the pipeline-migration skill to validate migrated Fabric pipelines in workspace
<fabric-workspace-name>. Compare source and target activity counts, notebook references,
connection references, variable library references, and parked activities. Do not
recreate triggers or schedules.
```
