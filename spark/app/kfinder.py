from pyspark.ml.clustering import KMeans
from pyspark.sql import SparkSession
from pyspark.ml.evaluation import ClusteringEvaluator 
import sys

K_START = int(sys.argv[1])
K_END = int(sys.argv[2])

spark = SparkSession.builder.appName('FindK').getOrCreate()

df = spark.read.load('/experiment/pca-data')
feature = df.sample(0.001)

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

result = spark.createDataFrame(zip(K, wcss, silhouette_scores), ['k', 'wcss', 'silhouette_score'])
result.write.mode('overwrite').save('/experiment/silhouette-score')

url = 'jdbc:postgresql://db:5432/postgres'
properties = {"user": "user", "password": "password", "driver": "org.postgresql.Driver"}
result.write.jdbc(url=url, table='elbow-score', mode='overwrite', properties=properties)
