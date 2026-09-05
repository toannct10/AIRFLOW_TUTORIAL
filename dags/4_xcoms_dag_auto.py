from airflow.sdk import dag,task

@dag(
    dag_id='xcom_dag_auto'
)

def xcom_dag_auto():
    @task.python
    def extract():
        example_data = {'data': [1,2,3,4,5]}
        return example_data

    @task.python
    def transform(data:dict):
        transformed_data = {'trans_data':data['data'] * 2 }
        return transformed_data


    @task.python
    def load(data):
        loaded_data = data
        return loaded_data

    extract = extract()
    transform = transform(extract)
    load = load(transform)

xcom_dag_auto()
