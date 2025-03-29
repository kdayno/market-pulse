from airflow.decorators import dag
from airflow.providers.databricks.operators.databricks import DatabricksNotebookOperator
from airflow.providers.databricks.operators.databricks_workflow import DatabricksWorkflowTaskGroup
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from pendulum import datetime
from datetime import timedelta

DATABRICKS_LOGIN_EMAIL = "kdayno@gmail.com"

DATABRICKS_ETL_NOTEBOOKS_PATH = f"/Users/{DATABRICKS_LOGIN_EMAIL}/market-pulse/etl"
DATABRICKS_TEST_NOTEBOOKS_PATH = f"/Users/{DATABRICKS_LOGIN_EMAIL}/market-pulse/tests/data-quality-tests"

DATABRICKS_JOB_CLUSTER_KEY = "market-pulse-job-cluster"
DATABRICKS_CONN_ID = "databricks_conn"

job_cluster_spec = [
    {
        "job_cluster_key": DATABRICKS_JOB_CLUSTER_KEY,
        "new_cluster": {
            "cluster_name": "",
            "spark_version": "16.2.x-scala2.12",
            "aws_attributes": {
                "first_on_demand": 1,
                "availability": "SPOT_WITH_FALLBACK",
                "zone_id": "auto",
                "spot_bid_price_percent": 100,
                "ebs_volume_count": 0,
            },
            "node_type_id": "m5d.large",
            "spark_env_vars": {"PYSPARK_PYTHON": "/databricks/python3/bin/python3"},
            "enable_elastic_disk": False,
            "data_security_mode": "DATA_SECURITY_MODE_AUTO",
            "kind": "CLASSIC_PREVIEW",
            "runtime_engine": "PHOTON",
            "num_workers": 1,
        },
    }
]


@dag(
    description="Triggers the multi-step ETL process for S&P500 Stock price data then triggers dbt DAG that aggregates the data",
    default_args = {
        "owner": "Kevin Dayno",
        "retries": 0,
        "execution_timeout": timedelta(hours=1),
    },
    start_date=datetime(2025, 1, 1),
    max_active_runs=1,
    schedule_interval="@daily",
    catchup=False,
    tags=['Market Pulse']
    )
def SP500_Stock_Prices_ETL():

    task_group = DatabricksWorkflowTaskGroup(
        group_id="SP500_Stock_Prices_ETL",
        databricks_conn_id=DATABRICKS_CONN_ID,
        job_clusters=job_cluster_spec,
        notebook_params={"input_load_date": "{{ ds }}"},
        notebook_packages=[ {'pypi':{'package': 'boto3'}},
                            {'pypi':{'package': 'python-dotenv'}},
                            {'pypi':{'package':'requests'}},
                            {'pypi':{'package':'polygon-api-client'}},
                            {'pypi':{'package':'asyncpraw'}},
                            {'pypi':{'package':'dbt-core'}},
                            {'pypi':{'package':'dbt-databricks'}},
                            {'pypi':{'package':'sqlfluff'}},
                            ]
    )

    with task_group:
        extract_polygon_stock_prices = DatabricksNotebookOperator(
            task_id="extract_polygon_stock_prices",
            databricks_conn_id=DATABRICKS_CONN_ID,
            notebook_path=f"{DATABRICKS_ETL_NOTEBOOKS_PATH}/extract/extract_polygon_stock_prices",
            source="WORKSPACE",
            job_cluster_key=DATABRICKS_JOB_CLUSTER_KEY
        )
        
        dq_tests_bronze_SP500_stock_prices = DatabricksNotebookOperator(
            task_id="dq_tests_bronze_SP500_stock_prices",
            databricks_conn_id=DATABRICKS_CONN_ID,
            notebook_path=f"{DATABRICKS_TEST_NOTEBOOKS_PATH}/dq_tests_bronze_SP500_stock_prices",
            source="WORKSPACE",
            job_cluster_key=DATABRICKS_JOB_CLUSTER_KEY
        )
        
        transform_SP500_stock_prices = DatabricksNotebookOperator(
            task_id="transform_SP500_stock_prices",
            databricks_conn_id=DATABRICKS_CONN_ID,
            notebook_path=f"{DATABRICKS_ETL_NOTEBOOKS_PATH}/transform/transform_SP500_stock_prices",
            source="WORKSPACE",
            job_cluster_key=DATABRICKS_JOB_CLUSTER_KEY,
        )

        SP500_stock_prices_avg_agg = DatabricksNotebookOperator(
            task_id="SP500_stock_prices_avg_agg",
            databricks_conn_id=DATABRICKS_CONN_ID,
            notebook_path=f"{DATABRICKS_ETL_NOTEBOOKS_PATH}/load/SP500_stock_prices_avg_agg",
            source="WORKSPACE",
            job_cluster_key=DATABRICKS_JOB_CLUSTER_KEY,
        )

    trigger_SP500_stock_prices_agg_dbt = TriggerDagRunOperator(
        task_id="Trigger_SP500_Stock_Prices_Avg_Agg",
        trigger_dag_id="SP500_Stock_Prices_Agg_dbt",
        wait_for_completion=True,
        deferrable=True,  # Note that this parameter only exists in Airflow 2.6+
    )

    (extract_polygon_stock_prices 
    >> dq_tests_bronze_SP500_stock_prices 
    >> transform_SP500_stock_prices 
    >> SP500_stock_prices_avg_agg 
    >> trigger_SP500_stock_prices_agg_dbt)

SP500_Stock_Prices_ETL()
