from datetime import timedelta, datetime
from airflow import DAG
# Operators; we need this to operate!
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import SparkKubernetesSensor
from airflow.utils.dates import days_ago


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email': ['teste_teste.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'max_active_runs': 1,
    'retries': 1
}

dag = DAG(
    'Test-App-Spark_v2',
    default_args=default_args,
    schedule_interval=timedelta(days=1),
    tags=['sparkapplication'],
)

submit = SparkKubernetesOperator(
    task_id='spark_transform_data',
    namespace="default",
    application_file="sparkoperator-app01.yaml",
    kubernetes_conn_id="k8s",
    do_xcom_push=False,
    dag=dag
)

sensor = SparkKubernetesSensor(
   task_id='spark_app_monitor',
   namespace="default",
   application_name="{{ task_instance.xcom_pull(task_ids='spark_transform_data')['metadata']['name'] }}",
   kubernetes_conn_id="k8s",
   poke_interval=30,  # Intervalo entre as tentativas de verificação (em segundos)
   timeout=600,  # Tempo máximo para aguardar o job (em segundos)
   dag=dag,
   attach_log=True,
)

submit >> sensor