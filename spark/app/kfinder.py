from pyspark.ml.feature import VectorAssembler, StandardScaler, PCA
from pyspark.ml.clustering import KMeans
from pyspark.sql import SparkSession
from pyspark.ml.evaluation import ClusteringEvaluator 
import sys

K_START = int(sys.argv[1])
K_END = int(sys.argv[2])

spark = SparkSession.builder.appName('FindK').getOrCreate()

df = spark.read.load('/transformed-data')
df = df.na.drop()
df = df.sample(0.001)

columns = [
  # 'vendor__cmt',
  # 'vendor__vf',
  'trip_distance',
  'passenger_count',
  # 'payment_type__credit_card',
  # 'payment_type__cash',
  # 'payment_type__no_charge',
  # 'payment_type__dispute',
  # 'payment_type__unknown',
  'total_amount',
  'fare_per_passenger',
  'fare_per_distance',
  'fare_per_duration',
  # 'month',
  # 'day',
  # 'hour',
  'duration',
]

vectorizer = VectorAssembler(inputCols=columns, outputCol='vector')
vector = vectorizer.transform(df)

scaler = StandardScaler(inputCol="vector", 
                        outputCol="scaled_features",
                        withStd=True,
                        withMean=True)
scaled_feature = scaler.fit(vector).transform(vector)

pca = PCA(k=2, inputCol="scaled_features", outputCol="features")
feature = pca.fit(scaled_feature).transform(scaled_feature)

silhouette_score=[] 
  
evaluator = ClusteringEvaluator(predictionCol='cluster', 
                                featuresCol='features') 

wcss = []
silhouette_scores = []
K = list(range(K_START, K_END+1))
for k in K:
  kmeans = KMeans(k=k, featuresCol='features', predictionCol='cluster')
  model = kmeans.fit(feature)
  cost = model.summary.trainingCost
  clusters = model.transform(feature)
  silhouette_score = evaluator.evaluate(clusters)
  
  wcss.append(cost)
  silhouette_scores.append(silhouette_score)

# create spark df with k and silhouette score
result = spark.createDataFrame(zip(K, wcss, silhouette_scores), ['k', 'silhouette_score'])
result.write.mode('overwrite').save('/experiment/silhouette-score')

# dump experiment results             
feature.select('id', 'scaled_features').write.mode('overwrite').save('/experiment/scaled-features')
feature.select('id', 'features').write.mode('overwrite').save('/experiment/pca-features')
