from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

spark = SparkSession.builder \
    .appName("CombineSample") \
    .getOrCreate()

transformed_df = spark.read.load("/transformed-data")
clustered_df = spark.read.load("/clustered-data")

combined_df = transformed_df.join(clustered_df, on="id")
window_spec = Window.partitionBy("cluster").orderBy("id")
combined_df = combined_df.withColumn("row_number", row_number().over(window_spec))
sampled_df = combined_df.filter(col("row_number") <= 1000).drop("row_number")

url = 'jdbc:postgresql://db:5432/postgres'
properties = {"user": "postgres.tdqmcsbhunldlsnswdlv", "password": "Dg3ddry3BgcfwvjA", "driver": "org.postgresql.Driver"}
sampled_df.write.jdbc(url=url, table='cluster', mode='overwrite', properties=properties) 
