from airflow.decorators import task, dag
from airflow.operators.dummy import DummyOperator
from airflow.models import Variable
from datetime import datetime
import boto3


aws_access_key_id = Variable.get('aws_access_key_id')
aws_secret_access_key = Variable.get('aws_secret_access_key')

client = boto3.client(
    'emr', region_name='us-east-1',
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key
)

default_args = {
    'owner': 'Carlos e Ezequiel',
    'start_date': datetime(2022, 6, 11)
}


@dag(default_args=default_args, schedule_interval="@once", description="Executa um job Spark no EMR", catchup=False, tags=['Spark', 'EMR'])
def indicadores_aereos():

    inicio = DummyOperator(task_id='inicio')

    @task
    def tarefa_inicial():
        print("Começou!!")

    # Cria um cluster EMR

    @task
    def emr_create_cluster():
        cluster_id = client.run_job_flow(
            Name='Automated_EMR_Carlos_Ezequiel',
            ServiceRole='EMR_DefaultRole',
            JobFlowRole='EMR_EC2_DefaultRole',
            VisibleToAllUsers=True,
            LogUri='s3://aws-logs-104346215011-us-east-1/elasticmapreduce/',
            ReleaseLabel='emr-6.8.0',
            Instances={
                'InstanceGroups': [
                    {
                        'Name': 'Master nodes',
                        'Market': 'ON_DEMAND',
                        'InstanceRole': 'MASTER',
                        'InstanceType': 'm5.xlarge',
                        'InstanceCount': 1,
                    },
                    {
                        'Name': 'Worker nodes',
                        'Market': 'ON_DEMAND',
                        'InstanceRole': 'CORE',
                        'InstanceType': 'm5.xlarge',
                        'InstanceCount': 1,
                    }
                ],
                'Ec2KeyName': 'ce-pucminas-testes',
                'KeepJobFlowAliveWhenNoSteps': True,
                'TerminationProtected': False,
                'Ec2SubnetId': 'subnet-0930242c8c582244d'
            },

            Applications=[{'Name': 'Spark'}, {'Name': 'Hive'}],
        )
        return cluster_id["JobFlowId"]

    # espera o cluster esta de pé
    @task
    def wait_emr_cluster(cid: str):
        waiter = client.get_waiter('cluster_running')

        waiter.wait(
            ClusterId=cid,
            WaiterConfig={
                'Delay': 30,
                'MaxAttempts': 60
            }
        )
        return True

    # joga o processamento para o EMR dos jobs spark

    @task
    def emr_process_aereos(cid: str):
        newstep = client.add_job_flow_steps(
            JobFlowId=cid,
            Steps=[
                {
                    'Name': 'Processa indicadores aereos parte 1',
                    'ActionOnFailure': "CONTINUE",
                    'HadoopJarStep': {
                        'Jar': 'command-runner.jar',
                        'Args': ['spark-submit',
                                 '--master', 'yarn',
                                 '--deploy-mode', 'cluster',
                                 's3://emr-code-104346215011/pyspark/job_spark_indicadores_part_1.py'
                                 ]
                    }
                }
            ]
        )
        return newstep['StepIds'][0]

    # espera o  step do job terminar
    @task
    def wait_emr_job(cid: str, stepId: str):
        waiter = client.get_waiter('step_complete')

        waiter.wait(
            ClusterId=cid,
            StepId=stepId,
            WaiterConfig={
                'Delay': 10,
                'MaxAttempts': 600
            }
        )

    @task
    def terminate_emr_cluster(cid: str):
        res = client.terminate_job_flows(
            JobFlowIds=[cid]
        )

    fim = DummyOperator(task_id="fim")

    # Orquestração
    tarefainicial = tarefa_inicial()
    cluster = emr_create_cluster()
    inicio >> tarefainicial >> cluster

    esperacluster = wait_emr_cluster(cluster)

    indicadores = emr_process_aereos(cluster)
    esperacluster >> indicadores

    wait_step = wait_emr_job(cluster, indicadores)

    terminacluster = terminate_emr_cluster(cluster)
    wait_step >> terminacluster >> fim


execucao = indicadores_aereos()
