import os
from datetime import datetime, timedelta
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, RenderConfig, ExecutionConfig
from airflow.operators.empty import EmptyOperator
from airflow.decorators import dag

MARKET_PULSE_DIR_PATH = os.getcwd()

PATH_TO_DBT_PROJECT = f"{MARKET_PULSE_DIR_PATH}/market_pulse_dbt"
PATH_TO_DBT_PROFILES = f"{MARKET_PULSE_DIR_PATH}/market_pulse_dbt/profiles.yml"

profile_config = ProfileConfig(
    profile_name="market_pulse_dbt",
    target_name="dev",
    profiles_yml_filepath=PATH_TO_DBT_PROFILES,
)



@dag(
    description='Triggers dbt DAG that aggregates S&P500 financials data and creates gold table',
    default_args = {
        "owner": "Kevin Dayno",
        "retries": 0,
        "execution_timeout": timedelta(hours=1),
    },
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=['Market Pulse', 'dbt']
    )
def SP500_Financials_Agg_dbt():

    # Pre DBT workflow task
    pre_dbt_workflow = EmptyOperator(task_id="pre_dbt_workflow")

    dbt_run_staging = DbtTaskGroup(
        group_id="market_pulse_dbt_dag",
        project_config=ProjectConfig(PATH_TO_DBT_PROJECT),
        profile_config=profile_config,
        execution_config=ExecutionConfig(
            dbt_executable_path=f"./.venv/bin/dbt",
        ),
        render_config=RenderConfig(
            select=["gold_sp500_stock_financial_ratios_agg"],
        ),
    )

    # Post DBT workflow task
    post_dbt_workflow = EmptyOperator(task_id="post_dbt_workflow", trigger_rule="all_done")

    # Task dependencies
    pre_dbt_workflow >> dbt_run_staging >> post_dbt_workflow

SP500_Financials_Agg_dbt()