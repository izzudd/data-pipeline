FROM ubuntu:22.04 as hadoop-base

ARG HADOOP_VERSION=3.4.0

# Install tools required by the OS
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
    sudo \
    openssh-server \
    python3 \
    python3-pip \
    ssh \
    pdsh \
    curl \
    openjdk-11-jdk \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

# Setup ssh
RUN echo "export PDSH_RCMD_TYPE=ssh" > ~/.bashrc \
  && ssh-keygen -t rsa -P "" -f ~/.ssh/id_rsa \
  && cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys

# Download hadoop
ENV HADOOP_HOME="/root/hadoop"
RUN curl https://dlcdn.apache.org/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz -o hadoop.tar.gz \
  && mkdir $HADOOP_HOME && tar xzf hadoop.tar.gz --directory $HADOOP_HOME --strip-components 1 \
  && rm -f hadoop.tar.gz

# Setup hadoop

ENV JAVA_HOME="/usr/lib/jvm/java-11-openjdk-amd64"
ENV PATH="${HADOOP_HOME}/bin:${PATH}"
ENV PATH="${HADOOP_HOME}/sbin:${PATH}"
ENV HADOOP_MAPRED_HOME="${HADOOP_HOME}"
ENV HADOOP_COMMON_HOME="${HADOOP_HOME}"
ENV HADOOP_HDFS_HOME="${HADOOP_HOME}"
ENV YARN_HOME="${HADOOP_HOME}"
ENV HDFS_NAMENODE_USER="root"
ENV HDFS_DATANODE_USER="root"
ENV HDFS_SECONDARYNAMENODE_USER="root"
ENV YARN_RESOURCEMANAGER_USER="root"
ENV YARN_NODEMANAGER_USER="root"
ENV PDSH_RCMD_TYPE="ssh"

RUN echo "export JAVA_HOME=${JAVA_HOME}" >> $HADOOP_HOME/etc/hadoop/hadoop-env.sh \
  && echo "export PDSH_RCMD_TYPE=ssh" >> $HADOOP_HOME/etc/hadoop/hadoop-env.sh \
  && mkdir -p ${HADOOP_HOME}/data && chmod 700 ${HADOOP_HOME}/data
  
WORKDIR /root
COPY entrypoint.sh .
RUN chmod +x entrypoint.sh
COPY app ./app

ENTRYPOINT ["./entrypoint.sh"]