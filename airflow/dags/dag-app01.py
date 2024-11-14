# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
# Operators; we need this to operate!
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import SparkKubernetesSensor
from airflow.providers.cncf.kubernetes.hooks.kubernetes import KubernetesHook
from airflow.utils.dates import days_ago
k8s_hook = KubernetesHook(conn_id='kubernetes_config')

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email': ['teste@teste.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'max_active_runs': 1,
    'retries': 3
}

dag = DAG(
    'Test-App-Spark',
    start_date=days_ago(1),
    default_args=default_args,
    schedule_interval=timedelta(days=1),
    tags=['sparkapplication']
)

submit = SparkKubernetesOperator(
    task_id='spark_transform_data',
    namespace='spark-operator',
    application_file='sparkoperator-app01.yaml',
    kubernetes_conn_id='k8s',
    do_xcom_push=True,
)


submit