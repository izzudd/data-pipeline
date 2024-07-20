from pyspark.ml.stat import Correlation
from pyspark.ml.feature import VectorAssembler 
from pyspark.ml.linalg import DenseMatrix
from pyspark.sql import SparkSession, Row

spark = SparkSession.builder.appName('CorrelationMap').getOrCreate()

df = spark.read.load('/transformed-data')
columns = columns = [
  'trip_distance',
  'passenger_count',
  'total_amount',
  'fare_amount',
  'extra',
  'mta_tax',
  'tip_amount',
  'tolls_amount',
  'improvement_surcharge',
  'congestion_surcharge',
  'airport_fee',
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

correlation = Correlation.corr(vector, 'vector', 'pearson')
correlation_matrix = correlation.collect()[0][0]

def flatten_correlation_matrix(feature_names, correlation_matrix):
    matrix = correlation_matrix.toArray().tolist()
    
    rows = []
    for i, row in enumerate(matrix):
      for j, correlation_score in enumerate(row):
        rows.append(Row(v1=feature_names[i], v2=feature_names[j], correlation_score=correlation_score))
    
    df = spark.createDataFrame(rows)
    return df

correlation_df = flatten_correlation_matrix(columns, correlation_matrix)
correlation_df.write.mode('overwrite').save('/experiment/correlation-map')

url = 'jdbc:postgresql://db:5432/postgres'
properties = {"user": "user", "password": "password", "driver": "org.postgresql.Driver"}
correlation_df.write.jdbc(url=url, table='correlation', mode='overwrite', properties=properties)
