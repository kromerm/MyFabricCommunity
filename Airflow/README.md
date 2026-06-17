# Fabric Airflow and dbt: NYC Taxi Silver Layer Sample

A minimal working example of building a Fabric dbt Job and triggering it from an Apache Airflow Job in Microsoft Fabric Data Factory. Uses the NYC TLC public taxi dataset.

## What's in this folder

```
Fabric Airflow and dbt/
├── dbt_project.yml
├── models/
│   ├── sources.yml
│   └── silver_yellow_taxi_trips.sql
└── dags/
    └── dag_dbt_silver_taxi.py
```

`silver_yellow_taxi_trips.sql` filters bad rows from raw yellow taxi trip data, adds `trip_duration_minutes` and `tip_pct`, and joins the TLC zone lookup to resolve pickup and dropoff location IDs to borough and zone names. It's an incremental dbt model targeting a Fabric Warehouse.

`dag_dbt_silver_taxi.py` is an Airflow DAG that triggers the Fabric dbt Job using `MSFabricRunJobOperator`.

## Prerequisites

- A Fabric workspace with a **Warehouse** named `NYCTaxi_Warehouse`
- Two tables loaded into that Warehouse under the `dbo` schema:
  - `yellow_tripdata` — NYC TLC yellow cab trip data
  - `taxi_zone_lookup` — NYC TLC taxi zone lookup
- A Fabric **dbt Job** item with its adapter settings configured against `NYCTaxi_Warehouse`
- A Fabric **Apache Airflow Job** item with the `apache-airflow-microsoft-fabric-plugin` (preinstalled in Fabric Airflow Jobs)

## Setup

1. **Load the source tables.** Use a Copy Job (or any method you like) to land the TLC yellow taxi Parquet files and the zone lookup CSV into `NYCTaxi_Warehouse`, schema `dbo`.

2. **Create the dbt Job.** In your Fabric workspace, create a new dbt Job. When prompted for your first model file name, you can use this repo's `models/silver_yellow_taxi_trips.sql` as a starting point — copy its contents in.

3. **Configure the adapter.** Click **Configure adapter settings**, select **Warehouse**, and point it at `NYCTaxi_Warehouse`. Set the schema to `dbo`.

4. **Add `dbt_project.yml` and `models/sources.yml`.** Copy the contents from this repo into the corresponding files in the dbt Job editor.

5. **Run it once manually** to confirm the model builds and `silver_yellow_taxi_trips` appears in the Warehouse.

6. **Create the Airflow Job.** Add `dags/dag_dbt_silver_taxi.py` to your Fabric Apache Airflow Job's DAG folder.

7. **Set the required Airflow Variables** in the Airflow web UI (Admin > Variables):

   | Variable | Value |
   |---|---|
   | `fabric_workspace_id` | Your workspace GUID |
   | `dbt_silver_yellow_taxi_id` | The dbt Job item GUID |

8. **Trigger the DAG** and confirm it runs the dbt Job successfully.

## Notes

- This dbt Job targets a Fabric **Warehouse**, not a Lakehouse. The built-in dbt Job runtime currently uses the Fabric Warehouse adapter; a native Lakehouse adapter is planned for a future release.
- `job_type='dbt'` is the value `MSFabricRunJobOperator` expects for triggering a Fabric dbt Job.

## Want the full walkthrough?

This sample is intentionally bare-bones. For the complete story, including the bronze ingestion pipeline that feeds this Warehouse, the alternative Dataflows Gen2 and PySpark Notebook approaches to the same transformation, and why dbt Jobs can't be triggered the same way Dataflows Gen2 can — I'm currently authoring a complete end-to-end book: **Microsoft Fabric Data Factory** (Packt Publishing).

---

Questions or improvements? Open an issue or PR.
