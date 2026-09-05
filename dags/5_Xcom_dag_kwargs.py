from airflow.sdk import dag, task

@dag(
    dag_id = 'xcom_dag_kwargs'
)

def xcom_dag_kwargs():
    @task.python
    def first_task(**kwargs):
        ti = kwargs['ti']
        fetch_data = {'data':[1,2,3,4,5]}
        ti.xcom_push(key='return_result', value=fetch_data)

    @task.python
    def second_task(**kwargs):
        ti = kwargs['ti']

        data = ti.xcom_pull(task_ids='first_task', key='return_result')

        trans_data = {'data': data['data'] * 2}

        ti.xcom_push(key='return_result', value=trans_data)


    @task.python 
    def third_task(**kwargs):
        ti = kwargs['ti']
        load_data = ti.xcom_pull(task_ids='second_task',key='return_result')
        return load_data


    first = first_task()
    second = second_task()
    third = third_task()

    first >> second >> third

xcom_dag_kwargs()
