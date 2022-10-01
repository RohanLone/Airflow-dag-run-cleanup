# Airflow-dag-run-cleanup
Workflow to clean the DAG run instances


* Cleanup DAG Run Status
  * A maintenance workflow that you can deploy into Airflow to periodically clean out the DagRun, entries to avoid having too much data in your Airflow MetaStore.

Change variables in program:
`SCHEDULE_INTERVAL` `timedelta(days=30)` `DAG_OWNER_NAME` `START_DATE` `airflow_db_user_name` `airflow_db_password`

SCHEDULE_INTERVAL - Defines how often that DAG runs

timedelta(days=30) - Length to retain the DAG runs
