echo "<configuration>
    <property>
        <name>dfs.replication</name>
        <value>${HDFS_REPLICATION_FACTOR:-2}</value>
    </property>
    <property>
        <name>dfs.namenode.name.dir</name>
        <value>${HADOOP_HOME}/data/name_node</value>
    </property>
    <property>
        <name>dfs.datanode.data.dir</name>
        <value>${HADOOP_HOME}/data/data_node</value>
    </property>
</configuration>"