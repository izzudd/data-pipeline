from pyspark.sql import SparkSession

url = 'jdbc:postgresql://db:5432/postgres'
properties = {"user": "postgres.tdqmcsbhunldlsnswdlv", "password": "Dg3ddry3BgcfwvjA", "driver": "org.postgresql.Driver"}  

spark = SparkSession.builder.appName('SubmitTransormedData').getOrCreate()
raw_data = spark.read.load('/transformed-data')
raw_data.write.jdbc(url=url, table='transformed_data', mode='overwrite', properties=properties) 
spark.stop()

spark = SparkSession.builder.appName('SubmitClusteredData').getOrCreate()
clustered_data = spark.read.load('/clustered-data')
clustered_data.write.jdbc(url=url, table='clustered_data', mode='overwrite', properties=properties)
spark.stop()

spark = SparkSession.builder.appName('SubmitKScore').getOrCreate()
clustered_data = spark.read.load('/experiment/silhouette-score')
clustered_data.write.jdbc(url=url, table='k_score', mode='overwrite', properties=properties)
spark.stop()