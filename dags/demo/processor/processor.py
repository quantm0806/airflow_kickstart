def process(**kwargs):
    scan_result = kwargs['ti'].xcom_pull(task_ids='scanner', key='return_value')
    print('question id: ', scan_result[0][0])
    print('class name: ', scan_result[0][1] + 'hello world')
