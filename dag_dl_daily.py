import os

from datetime import timedelta
from airflow.utils.dates import days_ago
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.models import Variable
from decimal import Decimal
import pandas as pd
import pendulum
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from subprocess import PIPE, Popen

DAG_NAME = os.path.basename(__file__).replace(".py", "")  # Le nom du DAG est le nom du fichier

default_args = {
    'owner': 'admin',
    'retries': 1,
    'retry_delay': timedelta(seconds=10)
}


@dag(DAG_NAME, default_args=default_args, schedule_interval="0 0 * * *", start_date=days_ago(2))
def dag_projet():
    """
    Ce DAG est permet de calculer de telecharger de maniere journaliere des donnees
    """

    @task()
    def execute_command(date):
        print(date)
        put = Popen(["python3", "/requete.py", date], stdin=PIPE, bufsize=-1)
        put.communicate()

    # paths >> filepath >> load >> cleaning

    @task()
    def clean(date):
        current_date = pendulum.parse(date).isoformat()
        print("current date : ", current_date)
        # file_path ="hdfs:/b4e7ed5c-ee42-48a8-9e36-fc2543871326.pub.instances.scw.cloud://data/g6/raw/daily_data2_"+current_date[:10]+".csv"
        file_path = "hdfs://8de53d22-71e8-443c-b675-78414b8d54df.priv.instances.scw.cloud:8020/data/g6/raw/daily_data2_" + current_date[:10] + ".csv"

        file_path_out = "hdfs://8de53d22-71e8-443c-b675-78414b8d54df.priv.instances.scw.cloud:8020/data/g6/clean/daily_data2_" + current_date[:10] + ".parquet"

        spark = SparkSession.builder.getOrCreate()
        df = spark.read.option("delimiter", ";").csv(file_path, header=True)
        # df = pd.read_csv(file_path)
        # df["year"] = current_date[:4]
        df = df.withColumn("year", F.lit(current_date[:4]))
        df = df.withColumn("month", F.lit(current_date[5:7]))
        df = df.withColumn("day", F.lit(current_date[-2:]))
        # df["month"] = current_date[5:7]
        # df["day"] = current_date[-2:]
        # print("df : ", df.dtypes)
        # print("shape : ", df.shape)
        #
        df.show()
        df.write.partitionBy("year", "month", "day").parquet(file_path_out)
        # df.to_parquet(file_path_out, partition_col = ["year","month","day"])
        return

    @task
    def merge(date):
        current_date = pendulum.parse(date).isoformat()
        spark = SparkSession.builder.getOrCreate()
        print("loading 1 : ")
        file_path = "hdfs://8de53d22-71e8-443c-b675-78414b8d54df.priv.instances.scw.cloud:8020/data/g6/clean/daily_data2_" + current_date[
                                                                                                                                 :10] + ".parquet"
        print("loading 2 : ")
        merging_path = "hdfs://8de53d22-71e8-443c-b675-78414b8d54df.priv.instances.scw.cloud:8020/data/g6/clean/merging_df.csv"
        print("loading 3 : ")
        merging_path2 = "hdfs://8de53d22-71e8-443c-b675-78414b8d54df.priv.instances.scw.cloud:8020/data/g6/clean/merging_df2.csv"

        print("OP 1 : ")
        df1 = spark.read.parquet(file_path)
        print("OP 2 : ")
        df2 = spark.read.option("delimiter",";").csv(merging_path, header=True)
        print("OP 3 : ")
        df3 = spark.read.option("delimiter",";").csv(merging_path2,header=True)
        print("OP 4 : ")
        df_merged = df1.join(df2.select('iu_ac', 'c_qu'), on=['iu_ac'], how='left')
        print("OP 5 : ")
        df_merged = df_merged.join(df3.select('c_qu', 'l_qu', 'c_ar', 'geom', 'geom_x_y'), on=['c_qu'], how='left')
        #df_merged = df_merged.where((F.col('k') != ''))
        print("OP 6 : ")
        df_merged.coalesce(1).write.option("header","true").csv("hdfs://8de53d22-71e8-443c-b675-78414b8d54df.priv.instances.scw.cloud:8020/data/g6/out/"+current_date[:10])


    job = execute_command("{{ execution_date }}")
    cleaning_job = clean("{{ execution_date }}")
    merging_job = merge("{{ execution_date }}")


dag_projet_instances = dag_projet()  # Instanciation du DAG

# Pour run:
# airflow dags backfill --start-date 2019-01-02 --end-date 2019-01-03 --reset-dagruns dag_dl_daily
# airflow tasks test aggregate_data_2 2019-01-02

# COpie du dag dans airflow :
#cp dag_dl_daily.py /root/airflow/dags/
# Pour tester uniquement une fonction :
# airflow tasks test dag_dl_daily clean 2019-01-02

# On doit mkdir dans airflow si ya plus le dossier
# mkdir /root/aiflow/dags
# Copie de local to cluster :
# scp -i iabd /home/ttaranto/Travail/Automatisation/* root@b4e7ed5c-ee42-48a8-9e36-fc2543871326.pub.instances.scw.cloud:/root/g6