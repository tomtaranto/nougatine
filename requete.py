#!/usr/bin/env python
from pyspark.sql import SparkSession
import subprocess
from subprocess import PIPE, Popen
import requests
import datetime
from datetime import timedelta 
#import pandas as pd
import pendulum
#from pendulum import Pendulum

import csv
import sys

def utf_8_encoder(unicode_csv_data):
    for line in unicode_csv_data:
        yield line.encode('utf-8')

def init_spark():
  spark = SparkSession.builder.appName("HelloWorld").getOrCreate()
  sc = spark.sparkContext
  return spark,sc

def main(date):
	#current_date = datetime.datetime.now().replace(microsecond=0).replace(second=0).replace(minute=0).replace(hour=0).isoformat()
	#current_date += 'Z'
	current_date = date.isoformat()
	print(current_date)
	#previous_day = datetime.datetime.now() - timedelta(days=1)
	#previous_day = str(previous_day.replace(microsecond=0).replace(second=0).replace(minute=0).replace(hour=0)) + 'Z'
	#previous_day = previous_day.replace(" ", "T")
	previous_day = (date - timedelta(days=1)).isoformat()
	print(previous_day)
	query = 't_1h: [' + previous_day+' TO ' + current_date + ']'
	payload = {'limit':'10000',
           'q': query,
           'sort': '-t_1h',
           'facet': 't_1h'}
	print("Telechargement des donnees journalieres du "+current_date)
	res1 = requests.get('https://opendata.paris.fr/api/v2/catalog/datasets/comptages-routiers-permanents/exports/csv/',
	                  params=payload, verify=False)

	decoded_content = res1.content

	open('/tmp/daily_data_'+current_date[:10]+'.csv','wb').write(decoded_content)

	#df = pd.DataFrame(my_list)
	#df = df.rename(columns=df.iloc[0]).drop(0)
	#df = df.reset_index(drop=True)
	#print(df.shape)
	#df.head(5)
	#df.to_csv("daily_data.csv", index=False)
	#spark,sc = init_spark()
	local_file = '/tmp/daily_data_'+current_date[:10]+'.csv'
	print(local_file)
	hdfs_file = "hdfs:///data/g6/raw/daily_data2_"+current_date[:10]+".csv"
	put = Popen(["hadoop", "fs", "-copyFromLocal", local_file, hdfs_file], stdin=PIPE, bufsize=-1)
	print("Writing data to hdfs")
	#subprocess.call(["hadoop fs -copyFromLocal   hdfs:///data/g6/raw/daily_data2.csv"], shell=True) # pas sur du chemin HDFS
	put.communicate()

if __name__ == '__main__':
	if len(sys.argv)>0:
		print("date : ",sys.argv[1])
		#print('format iso : ',datetime.fromisoformat(sys.argv[1]))
		print('format date : ',pendulum.parse(sys.argv[1]))
		print("au format requete : ",pendulum.parse(sys.argv[1]).replace(microsecond=0).replace(second=0).replace(minute=0).replace(hour=0).isoformat())
	main(pendulum.parse(sys.argv[1]).replace(microsecond=0).replace(second=0).replace(minute=0).replace(hour=0))


#airflow tasks test dag_dl_daily execute_command 2021-12-15
#yes | cp -f dag_dl_daily.py /root/airflow/dags/


