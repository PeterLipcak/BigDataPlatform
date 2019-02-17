FROM ubuntu:16.04

RUN apt-get update && apt-get install -y \
 wget \
 default-jdk \
 python2.7 \
 scala && cd /home && mkdir spark && cd spark && \
 wget http://artfiles.org/apache.org/spark/spark-2.4.0/spark-2.4.0-bin-hadoop2.7.tgz && \
 tar -xvf spark-2.4.0-bin-hadoop2.7.tgz

ENV JAVA_HOME /usr/lib/jvm/java-1.8.0-openjdk-amd64 
ENV SPARK_HOME /home/spark/spark-2.4.0-bin-hadoop2.7

ENTRYPOINT ["/home/spark/spark-2.4.0-bin-hadoop2.7/bin/spark-class","org.apache.spark.deploy.master.Master"]
