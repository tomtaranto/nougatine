from pyspark.sql import SparkSession
from subprocess import PIPE, Popen
import requests
import datetime
from datetime import timedelta 
#import pandas as pd

import csv

def utf_8_encoder(unicode_csv_data):
    for line in unicode_csv_data:
        yield line.encode('utf-8')

def init_spark():
  spark = SparkSession.builder.appName("HelloWorld").getOrCreate()
  sc = spark.sparkContext
  return spark,sc

def main():
	current_date = datetime.datetime.now().replace(microsecond=0).isoformat()
	current_date += 'Z'
	next_day = datetime.datetime.now() + timedelta(days=1)
	next_day = str(next_day.replace(microsecond=0)) + 'Z'
	next_day = next_day.replace(" ", "T")
	query = 't_1h: [' + current_date + ' TO ' + next_day + ']'
	payload = {'limit':'10000',
           'q': query,
           'sort': '-t_1h',
           'facet': 't_1h'}

	res1 = requests.get('https://opendata.paris.fr/api/v2/catalog/datasets/comptages-routiers-permanents/exports/csv/',
	                  params=payload, verify=False)

	decoded_content = res1.content.decode('utf-8')

	cr = csv.reader(utf_8_encoder(decoded_content.splitlines()), delimiter=';')
	my_list = list(cr)
	with open('daily_data.csv','w') as f:
		writer = csv.writer(f)
		for row in my_list:
			writer.writerow(row)
	#df = pd.DataFrame(my_list)
	#df = df.rename(columns=df.iloc[0]).drop(0)
	#df = df.reset_index(drop=True)
	#print(df.shape)
	#df.head(5)
	#df.to_csv("daily_data.csv", index=False)
	spark,sc = init_spark()
	put = Popen(["hdfs", "dfs", "-put", "daily_data.csv", "hdfs/data/g6/raw/daily_data.csv"], stdin=PIPE, bufsize=-1)
	put.communicate()

if __name__ == '__main__':
	main()