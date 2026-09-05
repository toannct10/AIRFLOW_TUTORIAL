from airflow.sdk import dag, task

@dag(
    dag_id ='versioning_dag'
)

def versioning_dag():
    @task.python
    def first_task():
        print("This is first task")

    @task.python
    def second_task():
        print("This is second task")

    @task.python
    def third_task():
        print("This is third task")

    @task.python
    def version_task():
        print("This is version 3. DAG 3.0")

    first = first_task()
    second = second_task()
    third = third_task()
    version = version_task()
    first >> second >> third >> version

versioning_dag()