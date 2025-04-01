from airflow.decorators import dag
from airflow.providers.databricks.operators.databricks import DatabricksNotebookOperator
from airflow.providers.databricks.operators.databricks_workflow import DatabricksWorkflowTaskGroup
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from pendulum import datetime
from datetime import timedelta
from include.databricks_job_cluser_config import job_cluster_ml_spec

DATABRICKS_LOGIN_EMAIL = "kdayno@gmail.com"

DATABRICKS_NOTEBOOKS_PATH = f"/Users/{DATABRICKS_LOGIN_EMAIL}/market-pulse/etl"

DATABRICKS_JOB_CLUSTER_KEY = job_cluster_ml_spec[0]['job_cluster_key']
DATABRICKS_CONN_ID = "databricks_conn"

@dag(
    description="Triggers the multi-step ETL process for Reddit posts and sentiment data then triggers dbt DAG that aggregates the data",
    default_args = {
        "owner": "Kevin Dayno",
        "retries": 0,
        "execution_timeout": timedelta(hours=1),
    },
    start_date=datetime(2025, 1, 1),
    max_active_runs=1,
    schedule_interval="0 21 * * *", # Daily at 9PM UTC
    catchup=False,
    tags=['Market Pulse']
    )
def Reddit_Posts_Sentiment_ETL():

    databricks_workflow = DatabricksWorkflowTaskGroup(
        group_id="databricks_workflow",
        databricks_conn_id=DATABRICKS_CONN_ID,
        job_clusters=job_cluster_ml_spec,
        notebook_params={"input_subreddit_list": "['stocks', 'investing', 'trading', 'wallstreetbets']",
                        "input_hot_posts_time_filter": "month",
                        "input_top_posts_time_filter": "month"},
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
        extract_reddit_top_posts = DatabricksNotebookOperator(
            task_id="extract_reddit_top_posts",
            databricks_conn_id=DATABRICKS_CONN_ID,
            notebook_path=f"{DATABRICKS_NOTEBOOKS_PATH}/extract/extract_reddit_posts",
            source="WORKSPACE",
            job_cluster_key=DATABRICKS_JOB_CLUSTER_KEY
        )

        extract_reddit_hot_posts = DatabricksNotebookOperator(
            task_id="extract_reddit_hot_posts",
            databricks_conn_id=DATABRICKS_CONN_ID,
            notebook_path=f"{DATABRICKS_NOTEBOOKS_PATH}/extract/extract_reddit_posts",
            source="WORKSPACE",
            job_cluster_key=DATABRICKS_JOB_CLUSTER_KEY
        )

        transform_reddit_top_posts_sentiment_analysis = DatabricksNotebookOperator(
            task_id="transform_reddit_top_posts_sentiment_analysis",
            databricks_conn_id=DATABRICKS_CONN_ID,
            notebook_path=f"{DATABRICKS_NOTEBOOKS_PATH}/transform/transform_reddit_posts_sentiment_analysis",
            source="WORKSPACE",
            job_cluster_key=DATABRICKS_JOB_CLUSTER_KEY
        )

        transform_reddit_hot_posts_sentiment_analysis = DatabricksNotebookOperator(
            task_id="transform_reddit_hot_posts_sentiment_analysis",
            databricks_conn_id=DATABRICKS_CONN_ID,
            notebook_path=f"{DATABRICKS_NOTEBOOKS_PATH}/transform/transform_reddit_posts_sentiment_analysis",
            source="WORKSPACE",
            job_cluster_key=DATABRICKS_JOB_CLUSTER_KEY
        )

        dq_tests_silver_reddit_hot_posts_sentiment = DatabricksNotebookOperator(
            task_id="dq_tests_silver_reddit_hot_posts_sentiment",
            databricks_conn_id=DATABRICKS_CONN_ID,
            notebook_path=f"{DATABRICKS_NOTEBOOKS_PATH}/tests/data-quality-tests/dq_tests_silver_reddit_posts_sentiment",
            source="WORKSPACE",
            job_cluster_key=DATABRICKS_JOB_CLUSTER_KEY
        )

        dq_tests_silver_reddit_top_posts_sentiment = DatabricksNotebookOperator(
            task_id="dq_tests_silver_reddit_top_posts_sentiment",
            databricks_conn_id=DATABRICKS_CONN_ID,
            notebook_path=f"{DATABRICKS_NOTEBOOKS_PATH}/tests/data-quality-tests/dq_tests_silver_reddit_posts_sentiment",
            source="WORKSPACE",
            job_cluster_key=DATABRICKS_JOB_CLUSTER_KEY
        )

        transform_reddit_all_posts = DatabricksNotebookOperator(
            task_id="transform_reddit_all_posts",
            databricks_conn_id=DATABRICKS_CONN_ID,
            notebook_path=f"{DATABRICKS_NOTEBOOKS_PATH}/transform/transform_reddit_all_posts",
            source="WORKSPACE",
            job_cluster_key=DATABRICKS_JOB_CLUSTER_KEY,
        )

    trigger_reddit_posts_sentiment_agg_dbt = TriggerDagRunOperator(
        task_id="trigger_reddit_posts_sentiment_agg_dbt",
        trigger_dag_id="Reddit_Posts_Sentiment_Agg_dbt",
        wait_for_completion=True,
        deferrable=True,  # Note that this parameter only exists in Airflow 2.6+
    )

    (extract_reddit_top_posts 
    >> [extract_reddit_hot_posts, transform_reddit_top_posts_sentiment_analysis]
    >> transform_reddit_hot_posts_sentiment_analysis
    >> [dq_tests_silver_reddit_hot_posts_sentiment, dq_tests_silver_reddit_top_posts_sentiment]
    >> transform_reddit_all_posts
    >> trigger_reddit_posts_sentiment_agg_dbt)
    
Reddit_Posts_Sentiment_ETL()
