from pyspark.ml.clustering import KMeans
from pyspark.sql import SparkSession
import sys

K = int(sys.argv[1])

spark = SparkSession.builder.appName('Clustering').getOrCreate()

df = spark.read.load('/experiment/pca-data')

kmeans = KMeans(k=K, featuresCol='features', predictionCol='cluster')
model = kmeans.fit(df)

cluster = model.transform(df)

result = cluster.select('id', 'cluster')
result.write.mode('overwrite').save('/clustered-data')
