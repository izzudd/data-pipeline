from pyspark.ml.feature import VectorAssembler 
from pyspark.ml.feature import StandardScaler 
from pyspark.ml.clustering import KMeans
from pyspark.sql import SparkSession
import sys

K = sys.argv[1]

spark = SparkSession.builder.appName('Clustering').getOrCreate()

df = spark.read.load('/transformed-data')

columns = [
  'vendor__cmt',
  'vendor__vf',
  'trip_distance',
  'payment_type__credit_card',
  'payment_type__cash',
  'payment_type__no_charge',
  'payment_type__dispute',
  'payment_type__unknown',
  'total_amount',
  'fare_per_passenger',
  'fare_per_distance',
  'fare_per_duration',
  'month',
  'day',
  'hour',
  'duration',
]

vectorizer = VectorAssembler(inputCols=columns, outputCol='vector')
vector = vectorizer.transform(df)

scaler = StandardScaler(inputCol="vector", 
                        outputCol="features",
                        withStd=True,
                        withMean=True)
feature = scaler.fit(vector).transform(vector)

kmeans = KMeans(k=K, featuresCol='features', predictionCol='cluster')
model = kmeans.fit(feature)

cluster = model.transform(feature)

result = cluster.select('id', 'cluster')
result.write.mode('overwrite').save('/clustered-data')
