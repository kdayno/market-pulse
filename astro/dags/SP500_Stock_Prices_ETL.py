from airflow.decorators import dag
from airflow.providers.databricks.operators.databricks import DatabricksNotebookOperator
from airflow.providers.databricks.operators.databricks_workflow import DatabricksWorkflowTaskGroup
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from pendulum import datetime
from datetime import timedelta
from include.databricks_job_cluser_config import job_cluster_spec

DATABRICKS_LOGIN_EMAIL = "kdayno@gmail.com"

DATABRICKS_NOTEBOOKS_PATH = f"/Users/{DATABRICKS_LOGIN_EMAIL}/market-pulse/etl"

DATABRICKS_JOB_CLUSTER_KEY = job_cluster_spec[0]['job_cluster_key']
DATABRICKS_CONN_ID = "databricks_conn"

@dag(
    description="Triggers the multi-step ETL process for S&P500 Stock price data then triggers dbt DAG that aggregates the data",
    default_args = {
        "owner": "Kevin Dayno",
        "retries": 0,
        "execution_timeout": timedelta(hours=2),
    },
    start_date=datetime(2025, 1, 1),
    max_active_runs=1,
    schedule_interval="0 21 * * *", # Daily at 9PM UTC
    catchup=False,
    tags=['Market Pulse']
    )
def SP500_Stock_Prices_ETL():

    databricks_workflow = DatabricksWorkflowTaskGroup(
        group_id="databricks_workflow",
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

    with databricks_workflow:
        extract_polygon_stock_prices = DatabricksNotebookOperator(
            task_id="extract_polygon_stock_prices",
            databricks_conn_id=DATABRICKS_CONN_ID,
            notebook_path=f"{DATABRICKS_NOTEBOOKS_PATH}/extract/extract_polygon_stock_prices",
            source="WORKSPACE",
            job_cluster_key=DATABRICKS_JOB_CLUSTER_KEY
        )
        
        dq_tests_bronze_SP500_stock_prices = DatabricksNotebookOperator(
            task_id="dq_tests_bronze_SP500_stock_prices",
            databricks_conn_id=DATABRICKS_CONN_ID,
            notebook_path=f"{DATABRICKS_NOTEBOOKS_PATH}/tests/data-quality-tests/dq_tests_bronze_SP500_stock_prices",
            source="WORKSPACE",
            job_cluster_key=DATABRICKS_JOB_CLUSTER_KEY
        )
        
        transform_SP500_stock_prices = DatabricksNotebookOperator(
            task_id="transform_SP500_stock_prices",
            databricks_conn_id=DATABRICKS_CONN_ID,
            notebook_path=f"{DATABRICKS_NOTEBOOKS_PATH}/transform/transform_SP500_stock_prices",
            source="WORKSPACE",
            job_cluster_key=DATABRICKS_JOB_CLUSTER_KEY,
        )

    trigger_SP500_stock_prices_agg_dbt = TriggerDagRunOperator(
        task_id="trigger_SP500_stock_prices_agg_dbt",
        trigger_dag_id="SP500_Stock_Prices_Agg_dbt",
        wait_for_completion=True,
        deferrable=True,  # Note that this parameter only exists in Airflow 2.6+
    )

    (extract_polygon_stock_prices 
    >> dq_tests_bronze_SP500_stock_prices 
    >> transform_SP500_stock_prices 
    >> trigger_SP500_stock_prices_agg_dbt)

SP500_Stock_Prices_ETL()
