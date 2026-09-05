from airflow.sdk import dag, task
from airflow.providers.standard.operators.bash import BashOperator
@dag(
    dag_id ='operators_dag'
)

def operators_dag():
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

    @task.bash
    def bash_task_modern():
        return "echo https://airflow.apache.org/"

    bash_oldschool = BashOperator(
        task_id = "bash_oldschool",
        bash_command = "echo https://airflow.apache.org/"
    )
    bash_modern = bash_task_modern()
    first = first_task()
    second = second_task()
    first >> second  >> bash_modern >> bash_oldschool

operators_dag()