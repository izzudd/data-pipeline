#!/bin/bash

hdfs dfs -mkdir /guttenberg
hdfs dfs -put guttenberg/*.txt /guttenberg
hadoop jar hadoop-streaming.jar \
  -input /guttenberg \
  -output /guttenberg-wc \
  -mapper "python3 /root/app/mapper.py" \
  -reducer "python3 /root/app/reducer.py"
