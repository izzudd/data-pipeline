FROM ubuntu:22.04 as spark-base

ARG SPARK_VERSION=3.5.1

# Install tools required by the OS
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
    sudo \
    openssh-server \
    curl \
    vim \
    unzip \
    rsync \
    openjdk-11-jdk \
    ssh \
    python3 \
    python3-pip \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

# Setup the directories for our Spark and Hadoop installations
ENV SPARK_HOME=${SPARK_HOME:-"/opt/spark"}

RUN mkdir -p ${SPARK_HOME}
WORKDIR ${SPARK_HOME}

# Download and install Spark
RUN curl https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz -o spark-${SPARK_VERSION}-bin-hadoop3.tgz \
 && tar xvzf spark-${SPARK_VERSION}-bin-hadoop3.tgz --directory /opt/spark --strip-components 1 \
 && rm -rf spark-${SPARK_VERSION}-bin-hadoop3.tgz

# Install python deps
COPY requirements.txt .
RUN pip install -r requirements.txt

# Setup Spark related environment variables
ENV PATH="/opt/spark/sbin:/opt/spark/bin:${PATH}"
ENV SPARK_MASTER_HOST spark-master
ENV SPARK_MASTER_PORT 7077
ENV PYSPARK_PYTHON python3
ENV PYTHONPATH=$SPARK_HOME/python/:$PYTHONPATH

RUN chmod u+x /opt/spark/sbin/* \
  && chmod u+x /opt/spark/bin/* \
  && mkdir -p /opt/spark/spark-events

WORKDIR /root
COPY entrypoint.sh .
RUN chmod +x entrypoint.sh

ENTRYPOINT ["./entrypoint.sh"]
