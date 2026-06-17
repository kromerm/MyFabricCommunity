# dag_dbt_silver_taxi.py
# Triggers the Fabric dbt Job (DBT_Silver_YellowTaxi) that builds the
# silver_yellow_taxi_trips model in the Fabric Warehouse.
#
# Prerequisites:
#   - NYCTaxi_Warehouse with yellow_tripdata and taxi_zone_lookup tables loaded
#   - A Fabric dbt Job named DBT_Silver_YellowTaxi with the adapter configured
#     against NYCTaxi_Warehouse and this repo's models/ folder
#   - Airflow Variables: fabric_workspace_id, dbt_silver_yellow_taxi_id

from datetime import datetime, timedelta
from airflow.decorators import dag
from airflow.providers.microsoft.fabric.operators.run_item import MSFabricRunJobOperator

default_args = {
    'owner': 'data-engineering',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

@dag(
    dag_id='dag_dbt_silver_taxi',
    description='Runs the Fabric dbt Job to build the silver_yellow_taxi_trips model',
    schedule='0 7 2 * *',  # 07:00 UTC on the 2nd, after bronze ingestion completes
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=['silver', 'nyc-taxi', 'dbt'],
)
def dbt_silver_taxi():

    run_dbt_job = MSFabricRunJobOperator(
        task_id='run_dbt_silver_model',
        fabric_conn_id='fabric-integration',
        workspace_id='{{ var.value.fabric_workspace_id }}',
        item_id='{{ var.value.dbt_silver_yellow_taxi_id }}',
        job_type='dbt',
        timeout=1800,
    )

dag_instance = dbt_silver_taxi()
