from airflow.sdk import dag, task
from pendulum import datetime

@dag(
    dag_id ='scheduling_first_dag',
    start_date = datetime(year=2026, month=9, day=1, tz='Asia/Ho_Chi_Minh'),
    schedule='@daily',
    is_paused_upon_creation = False,
    catchup=True
)

def scheduling_first_dag():
    @task.python
    def first_task():
        print("This is first task")

    @task.python
    def second_task():
        print("This is second task")

    @task.python
    def third_task():
        print("This is third task")

    first = first_task()
    second = second_task()
    third = third_task()

    first >> second >> third

scheduling_first_dag()
