from airflow.sdk import dag, task
from pendulum import datetime
from airflow.timetables.interval import CronDataIntervalTimetable

@dag(
    dag_id= 'incremental_loading',
    schedule = CronDataIntervalTimetable('@daily', timezone='Asia/Ho_Chi_Minh'),
    start_date= datetime(year=2026, month=9, day=1, tz='Asia/Ho_Chi_Minh'),
    start_end= datetime(year=2026, month=9, day=5, tz='Asia/Ho_Chi_Minh'),
    catchup=True
)

def incremental_loading():
    @task.python 
    def incremental_load(**kwargs):
        date_interval_start = kwargs['date_interval_start']
        date_interval_end = kwargs['date_interval_end']
        print(f"Fetching data from {date_interval_start} to {date_interval_end}")

    @task.bash
    def incremental_process():
        return f"echo'Processing incremental data from {{ data_interval_start }} to {{ data_interval_end }}"


    loading = incremental_load()
    processing = incremental_process()

    loading >> processing

incremental_loading()

