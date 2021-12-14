from pyspark.sql import SparkSession

def init_spark():
  spark = SparkSession.builder.appName("HelloWorld").getOrCreate()
  sc = spark.sparkContext
  return spark,sc

def main():
  spark,sc = init_spark()
  print("HelloWorld")


if __name__ == '__main__':
	main()

#Commande : 
# spark-submit --master yarn --deploy-mode client test.py
# Copier : 
# scp -i iabd -r /home/ttaranto/Travail/Automatisation/ root@aa31e5ba-fbf8-4a6a-845d-d55f77fd154a.pub.instances.scw.cloud:/root/g6