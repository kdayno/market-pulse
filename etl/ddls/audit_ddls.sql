CREATE TABLE tabular.dataexpert.kdayno_audit_job_runs (
  job_id STRING,
  job_name STRING,
  input_params_at_runtime STRING,
  job_start_date STRING,
  job_start_datetime STRING,
  task_run_id STRING,
  task_name STRING,
  source_tables STRING,
  target_tables STRING)
USING delta
PARTITIONED BY (job_start_date)