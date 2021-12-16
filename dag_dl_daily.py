import os
import boto3
import botocore
import pandas as pd

from datetime import timedelta
from airflow.utils.dates import days_ago
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.models import Variable
from decimal import Decimal

DAG_NAME = os.path.basename(__file__).replace(".py", "")  # Le nom du DAG est le nom du fichier

AWS_ACCESS_KEY = Variable.get("AWS_ACCESS_KEY_ID")
AWS_SECRET_KEY = Variable.get("AWS_SECRET_ACCESS_KEY")
AWS_S3_BUCKET_NAME = Variable.get("AWS_S3_BUCKET_NAME")
default_args = {
    'owner': 'tom',
    'retries': 1,
    'retry_delay': timedelta(seconds=10)
}


@dag(DAG_NAME, default_args=default_args, schedule_interval="0 0 * * *", start_date=days_ago(2))
def dag_projet():
    """
    Ce DAG est permet de calculer de manière journaliere le CA moyen des chauffeurs de taxi par jour
    """

    # Charge les données depuis S3
    @task()
    def extract(
            date):
        s3 = boto3.client(
            "s3",
            aws_access_key_id=AWS_ACCESS_KEY,
            aws_secret_access_key=AWS_SECRET_KEY,
            region_name="eu-west-3"
        )
        month = date[-5:-3]
        year = date[0:4]
        s3_filename = 'yellow_tripdata_' + year + '-' + month + '.csv'
        local_filename = '/tmp/' + s3_filename
        print("s3_filename",s3_filename)
        if not os.path.isfile(local_filename): # TODO remove after testings
            s3.download_file('esginyprojectrawdata', s3_filename,
                             local_filename)
        return dict(local_filename=local_filename)

    @task()
    def transform(date,
                  paths=None):
        if paths is None:  # Pour tester
            paths = dict(local_filename='~/PycharmProjects/NY_Project/data/yellow_tripdata_2019-01.csv')
        print("paths", paths)
        # On charge le fichier en local
        monthly_data = pd.read_csv(paths["local_filename"], sep=",", header=0)

        # On passe les colonnes en date
        monthly_data["tpep_pickup_datetime"] = pd.to_datetime(monthly_data["tpep_pickup_datetime"])
        monthly_data["day"] = monthly_data["tpep_pickup_datetime"].dt.day

        # On recupere uniquement le jour qui nous interesse
        day = int(date[-2:])
        print("day : ", day)
        print(monthly_data)
        daily_data = monthly_data[monthly_data["day"] == day]
        daily_data["month"] = int(date[-5:-3])

        # Finalement, on groupe par vendeur et jour
        res = daily_data[["VendorID", "day", "month", "total_amount"]].groupby(
            by=["VendorID", "day"]).mean().reset_index()

        res.to_csv("/tmp/yellow_cab_1_"+str(day)+".csv", index=False)
        return "/tmp/yellow_cab_1_"+str(day)+".csv"

    @task()
    def load(filepath):  # TODO faire l'insertion dans la table dynamoDB
        if filepath is None:
            filepath = '~/PycharmProjects/NY_Project/data/yellow_tripdata_2019-01.csv'
        # On lit le csv temporaire
        df = pd.read_csv(filepath).head(n=100)

        # On instancie notre table dynamoDB
        dynamodb = boto3.resource(
            "dynamodb",
            aws_access_key_id=AWS_ACCESS_KEY,
            aws_secret_access_key=AWS_SECRET_KEY,
            region_name="eu-west-3"
        )
        table = dynamodb.Table("esginyprojectdaily1")

        def put_row(row, batch):
            id = str(int(row.name)+int(row["month"])*100+int(float(row["day"]))+1000*int(row["VendorID"]))
            batch.put_item(
                Item={
                    'vendorid': int(row["VendorID"]),
                    'day': int(row["day"]),
                    'month': int(row["month"]),
                    'total_amount': Decimal(str(row["total_amount"])),
                    'id' : id
                }
            )

        with table.batch_writer() as batch:
            df.apply(lambda x: put_row(x, batch), axis=1)
        return filepath

    @task()
    def clean(paths):
        os.remove(paths)
        return 1

    paths = extract("{{ yesterday_ds }}")
    filepath = transform("{{ yesterday_ds }}", paths)
    tmp_files = load(filepath)
    cleaning = clean(tmp_files)
    #paths >> filepath >> load >> cleaning
    


dag_projet_instances = dag_projet()  # Instanciation du DAG


# Pour run:
#airflow dags backfill --start-date 2019-01-02 --end-date 2019-01-03 --reset-dagruns daily_ml
# airflow tasks test aggregate_data_2 2019-01-02