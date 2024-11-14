# import libs
from airflow import DAG
from datetime import timedelta, datetime
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import 
SparkKubernetesOperator
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import 
SparkKubernetesSensor
from airflow.models import Variable
from kubernetes.client import models as k8s
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator

default_args={
   'depends_on_past': False,
   'email': ['teste@teste.com'],
   'email_on_failure': False,
   'email_on_retry': False,
   'retries': 1,
   'retry_delay': timedelta(minutes=5)
}

with DAG(
   'Test-App-Spark',
   default_args=default_args,
   description='dag sparkapplication app01',
   schedule_interval=timedelta(days=1),
   start_date=datetime(2024, 11, 12),
   catchup=False,
   tags=['sparkapplication']
) as dag:
   t1 = SparkKubernetesOperator(
       task_id='spark_transform_dat',
       trigger_rule="all_success",
       depends_on_past=False,
       retries=3,
       application_file="sparkoperator-app01.yaml",
       namespace="spark-jobs",
       kubernetes_conn_id="k8s",
       api_group="sparkoperator.k8s.io",
       api_version="v1beta2",
       do_xcom_push=True,
       dag=dag
   )