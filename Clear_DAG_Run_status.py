import airflow
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator


SCHEDULE_INTERVAL = "@weekly"
DAG_OWNER_NAME = "DAG_OWNER_NAME"
START_DATE = airflow.utils.dates.days_ago(1)

default_args = {
    'owner': DAG_OWNER_NAME,
    'depends_on_past': False,
    'start_date': START_DATE,
    'retries': 2,
    'email': [],
    'retry_delay': timedelta(seconds=10)
}

dag = DAG('Clear_DAG_Run_status', catchup=False, schedule_interval= SCHEDULE_INTERVAL, default_args=default_args)

def Clear_DAG_Run_status():
  import os
  import json
  import requests

  ENDPOINT_URL = "http://localhost:8080/"
  user_name = 'airflow_db_user_name'
  password = 'airflow_db_password'

  initial_data = {
    "dry_run": True,
    "task_ids": [],
    "only_failed": None,
    "only_running": False,
    "include_subdags": True,
    "include_parentdag": True,
    "reset_dag_runs": True,
  }

  def get_dags(ENDPOINT_URL_,u_name, pwd):
    dag_list = []
    req = requests.get(f"{ENDPOINT_URL_}api/v1/dags",auth=(u_name, pwd))
    dag_response = req.json()
    for dags in dag_response['dags']:
      dag_list.append(dags['dag_id'])
    print("Fetching Dag List Successful!")
    return dag_list

  def delete_run(ENDPOINT_URL_,u_name, pwd,run_id_,dag_name,run_number_):
    print('Deletion Started')
    req = requests.delete(f"{ENDPOINT_URL_}api/v1/dags/{dag_name}/dagRuns/{run_id_}",auth=(u_name, pwd))
    print('Deletion Completed: ',dag_name,run_number_)

  def get_tasks(ENDPOINT_URL_,u_name, pwd,dag_list_):
    tasks_dict = {}
    for dag in dag_list_:
      task_list = []
      req = requests.get(f"{ENDPOINT_URL_}api/v1/dags/{dag}/tasks",auth=(u_name, pwd))
      task_response=req.json()
      for task in task_response['tasks']:
        task_list.append(task['task_id'])
      tasks_dict[dag] = task_list
    print("Fetching Task Name Successful!")
    return tasks_dict

  dag_list = get_dags(ENDPOINT_URL,user_name,password)
  dag_task = get_tasks(ENDPOINT_URL,user_name,password,dag_list)

  def runs(ENDPOINT_URL_,u_name,pwd,initial_data,status_,dag_task_):
    try:
      last_month_date = datetime.today()- timedelta(days=30)
      for run_number,(key,value) in enumerate(dag_task_.items()):
        initial_data['only_failed'] = status_
        initial_data['task_ids'] = value
        req = requests.post(f"{ENDPOINT_URL_}api/v1/dags/{key}/clearTaskInstances",auth=(u_name, pwd),
                          data = json.dumps(initial_data),headers={'Content-Type': 'application/json'})
        instance_response = req.json()
        if len(instance_response['task_instances']):
          run_ids = set()
          for instance in instance_response['task_instances']:
            if(last_month_date>datetime.strptime(str(instance['execution_date'].split('T')[0]),"%Y-%m-%d")):
              run_ids.add(instance['dag_run_id'])
          run_ids_list = list(run_ids)
          if len(run_ids_list):
            for ids in run_ids_list:
              delete_run(ENDPOINT_URL_,u_name,pwd,ids,key,run_number)
          else:
              print(f'No Minimum DAG RUN STATUS for {key} to delete')
        else:
          print(f'No DAG RUN STATUS for {key}')
      return True
    except Exception as e:
      print(e)
      return False

  #Delete Success Run Task
  for status in [True,False]:
    print('Running')
    completion_status = runs(ENDPOINT_URL,user_name,password,initial_data,status,dag_task)
    if completion_status:
      print(f'Deletion of FAILED DAG RUN STATUS completed') if status == True else print(f'Deletion of DAG SUCCESS RUN STATUS completed')
    else:
      print('Error in RUN')
        
Clear_DAG_Run_status_task = PythonOperator(
    task_id='Clear_DAG_Run_status',
    python_callable=Clear_DAG_Run_status,
    dag=dag)

Clear_DAG_Run_status

