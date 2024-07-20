docker exec hadoop-master hdfs dfs -put data /raw-data
docker exec spark-master spark-submit app/transform.py
docker exec spark-master spark-submit --jars app/postgresql.jar app/preprocess.py
docker exec spark-master spark-submit --jars app/postgresql.jar app/correlation.py
docker exec spark-master spark-submit --jars app/postgresql.jar app/kfinder.py 2 10
docker exec spark-master spark-submit app/clustering.py 5
docker exec spark-master spark-submit --jars app/postgresql.jar app/submit.py
docker exec spark-master spark-submit --jars app/postgresql.jar app/sample.py