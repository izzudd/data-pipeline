from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.feature import PCA
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('PCA').getOrCreate()

df = spark.read.load('/transformed-data')
df = df.na.drop()

columns = [
  'trip_distance',
  'passenger_count',
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
                        outputCol="scaled_features",
                        withStd=True,
                        withMean=True)
scaled_feature = scaler.fit(vector).transform(vector)

pca = PCA(k=len(columns), inputCol="scaled_features", outputCol="features")
model = pca.fit(scaled_feature)
cum_variance = model.explainedVariance.cumsum()
n_components = len(cum_variance[cum_variance < 0.9])

pca = PCA(k=n_components, inputCol="scaled_features", outputCol="features")
feature = pca.fit(scaled_feature).transform(scaled_feature)

feature.select('id', 'features').write.mode('overwrite').save('/experiment/pca-data')

import pandas as pd
variance = pd.DataFrame({
  'explained_variance': model.explainedVariance,
  'cumulative_variance': cum_variance,
  'n_components': list(range(1, len(columns)+1))
})

variance = spark.createDataFrame(variance)
variance.write.mode('overwrite').save('/experiment/pca-variance')

url = 'jdbc:postgresql://db:5432/postgres'
properties = {"user": "user", "password": "password", "driver": "org.postgresql.Driver"}
variance.write.jdbc(url=url, table='pca_variance', mode='overwrite', properties=properties)
