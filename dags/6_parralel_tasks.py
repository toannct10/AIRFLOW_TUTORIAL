from airflow.sdk import dag, task

@dag(
    dag_id='parralel_dag'
)

def parralel_dag():
    @task.python 
    def extract_data(**kwargs):
        ti = kwargs['ti']

        extracted_data = {
                          'db_data': [1,2,3],
                          's3_data': [4,5,6],
                          'api_data': [7,8,9]
                          }
        ti.xcom_push(key='return_value', value=extracted_data)

    @task.python
    def transform_db_data(**kwargs):
        ti = kwargs['ti']

        db_data = ti.xcom_pull(task_ids='extract_data')['db_data'] 

        trans_db_data = [i*2 for i in db_data]

        ti.xcom_push(key='return_value', value=trans_db_data)

    @task.python
    def transform_s3_data(**kwargs):
        ti = kwargs['ti']

        s3_data = ti.xcom_pull(task_ids='extract_data')['s3_data']

        trans_s3_data = [i*100 for i in s3_data]

        ti.xcom_push(key='return_value', value=trans_s3_data)

    @task.python
    def transform_api_data(**kwargs):
        ti = kwargs['ti']

        api_data = ti.xcom_pull(task_ids='extract_data')['api_data']

        trans_api_data = [i*1000 for i in api_data]
        ti.xcom_push(key='return_value', value=trans_api_data)

    @task.bash
    def load_data(**kwargs):

        db_data = kwargs['ti'].xcom_pull(task_ids='transform_db_data')
        s3_data = kwargs['ti'].xcom_pull(task_ids='transform_s3_data')
        api_data = kwargs['ti'].xcom_pull(task_ids='transform_api_data')

        return f"echo 'Loaded data: {db_data}, {s3_data}, {api_data} '"

    data = extract_data()
    db_data= transform_db_data()
    s3_data =transform_s3_data()
    api_data = transform_api_data()
    loaded_data = load_data()

    data >> [db_data, s3_data, api_data] >> loaded_data

parralel_dag()

    