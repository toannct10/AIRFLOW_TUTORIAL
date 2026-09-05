from airflow.sdk import dag, task
from pendulum import datetime, duration
from airflow.timetables.trigger import DeltaTriggerTimetable
@dag(
    dag_id ='scheduling_delta_dag',
    start_date = datetime(year=2026, month=9, day=1, tz='Asia/Ho_Chi_Minh'),
    schedule= DeltaTriggerTimetable(duration(days=3)),
    end_date = datetime(year=2026, month=9, day=5, tz='Asia/Ho_Chi_Minh'),
    is_paused_upon_creation = False,
    catchup=True
)

def scheduling_delta_dag():
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

scheduling_delta_dag()
