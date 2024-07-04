#!/bin/bash

/etc/init.d/ssh start

TYPE=$1

for file in config/*; do
  # check if file exetension is .xml
  if [[ "$file" == *".xml" ]]; then
    echo "Copying $file"
    cat "$file" > "$HADOOP_HOME/etc/hadoop/$(basename $file)"
    continue
  fi

  # if file is sh, execute it
  echo "Executing $file"
  output_file="$HADOOP_HOME/etc/hadoop/$(basename ${file%.sh})"
  bash "$file" > "$output_file"
done

if [ "$TYPE" = "master" ]; then
  if [ ! -f "$HADOOP_HOME/data/name_node/current/VERSION" ]; then
    echo "Formatting HDFS"
    hdfs namenode -format
  fi
  
  echo "Starting hadoop cluster"
  start-dfs.sh
  hdfs dfs -mkdir -p /user/root   # default directory for root user
  hdfs dfs -chmod 0777 / # not secure, but ok for demo
s
  if [ "$HDFS_START_YARN" = "true" ]; then
    echo "Starting yarn"
    start-yarn.sh
  fi
fi

if [ "$TYPE" = "worker" ]; then
  echo "Starting hadoop worker"
fi

tail -f /dev/null
