# How to build a complete Fabric Apache Airflow Job DAG completey with GitHub Copilot using MCP

This sample walkthrough shows how you can use the Microsoft Fabric and Data Factory MCP to create a three-task Apache Airflow DAG entirely from GitHub Copilot prompts — no portal, no Python boilerplate, no hardcoded names.
You describe what you want in natural language. Copilot reasons through the naming, wires the MCP tool calls, generates the DAG Python inline, and pushes everything to Fabric.

The DAG you'll build does three things in sequence:

1. **Ingest data** — The best way to move data in Fabric is Copy Job. Call a Copy Job that pulls files from Azure Data Lake Storage Gen2 into a Lakehouse table
2. **Run a notebook** — a Fabric Notebook executes after the copy completes
3. **Refresh a semantic model** — the "My Sales Data" semantic model refreshes once the notebook finishes

Two MCP servers do the heavy lifting: the **DataFactory MCP** creates the Copy Job and the Airflow Job itself; the **Fabric MCP** creates the Notebook. The DAG Python is generated on the fly and pushed directly to Fabric — no file upload required.

---

## Prerequisites

- VS Code with GitHub Copilot (Agent mode)
- [DataFactory MCP server](https://aka.ms/DFMCPAirflow) installed and registered in your VS Code MCP settings (`Microsoft.DataFactory.MCP`, .NET 10.0+)
- [Fabric MCP server](https://aka.ms/FabricMCP) installed and registered in your VS Code MCP settings
- A Fabric workspace with an assigned capacity (F2 or higher — Free and PPU workspaces don't support Airflow Jobs)
- A Lakehouse in that workspace (you'll need its name; Copilot will discover it)
- An ADLS Gen2 account with files you want to ingest (connection details handy)
- A semantic model named **"My Sales Data"** already published in your workspace

Note that I am making a series of assumption here. I assume that you have a Notebook that will transform and process data. I am also assuming that you have a working semantic model that is being used for business analytics.
I intend this article to serve as a guide for you to follow to use to build your own Fabric Apache Airflow apps using this same Github Copilot technique.

---

## Step 1 — Authenticate to Fabric

Open a Copilot chat in Agent mode and authenticate to both MCP servers:

> "Authenticate to Fabric using the DataFactory MCP and the Fabric MCP."

Copilot calls `mcp_datafactorymc_authenticate_interactive` and the Fabric MCP equivalent in sequence. Both open a browser tab for Microsoft Entra ID sign-in. After you sign in once, both servers share the session — you won't be prompted again during this walkthrough.

---

## Step 2 — Create the Copy Job

You don't need to name the Copy Job yourself. Ask Copilot to create it and let it reason through a sensible name based on the source and destination:

> "Create a Copy Job in my Fabric workspace that moves files from my ADLS Gen2 account into a table in my Lakehouse. I don't care what you call it — pick a name that reflects what it does."

Copilot inspects the workspace, reads your available connections, and reasons through a name. Its chain-of-thought looks something like:

> *Source is ADLS Gen2, destination is a Lakehouse table. A name like `adls-to-lakehouse-ingest` is descriptive and follows the source→destination pattern used in this workspace. I'll use that.*

It then calls `mcp_datafactorymc_create_copy_job` with that name and wires the ADLS Gen2 source and Lakehouse destination automatically. The response returns a Copy Job ID — Copilot stores it in context for the DAG generation step.

> **Note:** The Copy Job is created as a Fabric item and will appear in your workspace. The ADLS Gen2 connection settings in this walkthrough are illustrative — a real copy requires a valid linked service and the correct file path configured in the Copy Job definition.

---

## Step 3 — Create the Notebook

Now create the Notebook that runs after the copy. Keep it simple — this is a placeholder to demonstrate Airflow orchestration, not production logic:

> "Create a Fabric Notebook in my workspace using the Fabric MCP. Keep it simple — just a hello world cell that prints a completion message. Pick a name for it."

Copilot uses the Fabric MCP to create a new Notebook item. It reasons through a name:

> *The notebook runs after the copy job as a post-ingestion step. A name like `post-ingest-notebook` makes its role in the pipeline clear.*

The Fabric MCP creates the Notebook with a single Python cell:

```python
print("Ingestion pipeline step complete.")
```

The response returns the Notebook ID and its workspace URL. Copilot holds onto the Notebook item ID — it needs it when building the DAG.

---

## Step 4 — Generate the DAG

Now the interesting part. Ask Copilot to generate the complete DAG Python that wires all three tasks together. You don't write a single line of Python. This is really where I wanted to focus your attention, as opposed to the silly logic of an Hello World Notebook :)

> "Generate a Python Airflow DAG that runs three tasks in order: first trigger the Copy Job you just created, then run the Notebook you just created, then refresh a semantic model called 'My Sales Data' in my workspace. Name the DAG something sensible. Use the IDs you already have in context."

Copilot's reasoning chain:

> *I have the Copy Job ID and the Notebook item ID from the previous steps. For the semantic model refresh I need the dataset ID — I'll query the workspace to find the semantic model named "My Sales Data" and retrieve its ID.*
>
> *The Fabric Airflow provider includes `MSFabricRunJobOperator`, which handles job submission, polling, and error surfacing natively — no custom Python or REST calls needed. I'll use `job_type="CopyJobs"` for the Copy Job task and `job_type="RunNotebook"` for the Notebook task.*
>
> *For the semantic model refresh I'll use `MSFabricRunJobOperator` with `job_type="RefreshDataset"` and the dataset ID I just resolved.*
>
> *All three tasks will use the `fabric-integration` Airflow connection for auth, set `deferrable=True` so the worker thread is freed while waiting, and chain with `>>` . I'll name the DAG `fabric-ingest-and-refresh` and schedule it daily.*

Copilot produces the full DAG Python and displays it inline in chat:

```python
from datetime import datetime, timedelta
from airflow import DAG
from microsoft.fabric.airflow.operators import MSFabricRunJobOperator.run_item

WORKSPACE_ID      = "<your-workspace-id>"
COPY_JOB_ID       = "<copy-job-id-from-step-2>"
NOTEBOOK_ID       = "<notebook-id-from-step-3>"
SEMANTIC_MODEL_ID = "<semantic-model-id-resolved-by-copilot>"

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="fabric-ingest-and-refresh",
    default_args=default_args,
    schedule_interval="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["fabric", "ingest", "semantic-model"],
) as dag:

    copy_task = MSFabricRunJobOperator(
        task_id="run_copy_job",
        fabric_conn_id="fabric-integration",
        workspace_id=WORKSPACE_ID,
        item_id=COPY_JOB_ID,
        job_type="CopyJob",
        timeout=600,
        deferrable=True,
    )

    notebook_task = MSFabricRunJobOperator(
        task_id="run_notebook",
        fabric_conn_id="fabric-integration",
        workspace_id=WORKSPACE_ID,
        item_id=NOTEBOOK_ID,
        job_type="Notebook",
        timeout=600,
        deferrable=True,
    )

    refresh_task = MSFabricRunJobOperator(
        task_id="refresh_semantic_model",
        fabric_conn_id="fabric-integration",
        workspace_id=WORKSPACE_ID,
        item_id=SEMANTIC_MODEL_ID,
        job_type="RefreshDataset",
        timeout=600,
        deferrable=True,
    )

    copy_task >> notebook_task >> refresh_task
```

The IDs Copilot resolved in earlier steps are substituted into the DAG automatically — you don't fill in any placeholders by hand.

> **About `MSFabricRunJobOperator`:** This operator ships with the Fabric Airflow provider package (`microsoft-fabric-airflow`). It handles job submission, async polling, and structured error reporting for any Fabric item type that supports on-demand job execution — Copy Jobs, Notebooks, and semantic model refreshes included. The `fabric-integration` Airflow connection stores your workspace credentials; Copilot will create that connection in the next step if it doesn't already exist.

---

## Step 5 — Create the Airflow Job and push the DAG

With the DAG generated, ask Copilot to create the Airflow Job and push the definition in one prompt:

> "Create a new Airflow Job in my workspace called 'Fabric Ingest Pipeline', then push the DAG you just generated as its definition."

Copilot calls `mcp_datafactorymc_create_airflow_job` to create the Airflow Job, then calls `mcp_datafactorymc_update_airflow_job_definition` to push the DAG Python. The MCP tool handles base64 encoding the payload before it reaches the Fabric API — you never touch that encoding step.

Once the second call returns successfully, the DAG `fabric-ingest-and-refresh` is live in your Fabric workspace and visible in the Airflow UI.

---

## Step 6 — Trigger and verify

Kick off the DAG manually to verify the wiring:

> "Trigger a manual run of the 'Fabric Ingest Pipeline' Airflow Job and tell me if it started successfully."

Copilot calls `mcp_datafactorymc_run_airflow_job` and reports back the run status. You can follow up at any point:

> "What's the current status of the DAG run?"

`mcp_datafactorymc_get_airflow_job` returns the current state. If a task fails, ask Copilot to pull the logs:

> "Get the logs for the failed task in my last DAG run."

The DataFactory MCP Airflow Logs tool retrieves the raw task logs and Copilot summarizes what went wrong — no Airflow UI required.

---

## What just happened

In six prompts you:

- Created a Copy Job in Fabric (DataFactory MCP)
- Created a Notebook in Fabric (Fabric MCP)
- Generated a three-task Python DAG inline — no file, no boilerplate
- Created an Airflow Job and pushed the DAG definition (DataFactory MCP)
- Triggered a manual run and retrieved status

Copilot handled all naming decisions, resolved the semantic model ID from your workspace, encoded the DAG payload, and sequenced the API calls. You described outcomes; the MCPs executed them.

---

## Next steps

- **Add error handling** — ask Copilot to add an `on_failure_callback` that posts to a Teams webhook
- **Parameterize the DAG** — ask Copilot to convert the hardcoded IDs to Airflow Variables so the same DAG works across environments
- **Check dependencies** — ask Copilot to verify that `microsoft-fabric-airflow` is installed in your Airflow Job environment and that the `fabric-integration` connection is configured with the right credentials before you run in production

For the full DataFactory MCP tool reference, see [aka.ms/DFMCPAirflow](https://aka.ms/DFMCPAirflow). For the Fabric MCP reference, see [aka.ms/FabricMCP](https://aka.ms/FabricMCP).
