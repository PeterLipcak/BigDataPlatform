# BigDataPlatform
This is a big data analysis platform suitable for smart grids context. It contains platform source codes and also implementation of anomaly detection algorithm using Apache Spark, Apache Flink, and Apache Storm.

# Installation and deployment
In the following sections we will briefly explain the installation process of big data technologies and our platform with commands for executeing them.
## Big data technologies setup
### Following technologies are required to be installed in specific directory:
- Apache Zookeeper v3.4.12 - /usr/local/zookeeper
- Apache Kafka v2.0.0 - /usr/local/kafka
- Apache Hadoop (Distributed File System) v2.8.5 - /usr/local/hadoop
- Apache Spark v2.4.1 - /usr/local/spark
- Apache Flink v1.7.1 - /usr/local/flink
- Apache Storm v1.2.2 - /usr/local/storm

### Running Zookeeper
> /usr/local/zookeeper/bin/zkServer.sh start

### Running Kafka
> ./bin/kafka-server-start.sh config/server.properties

### Running HDFS
> ./sbin/start-dfs.sh

### Running Spark
> spark-class org.apache.spark.deploy.master.Master

> spark-class org.apache.spark.deploy.worker.Worker node-master:7077

### Running Flink
> ./bin/start-cluster.sh

### Running Storm
> ./bin/storm nimbus

> bin/storm supervisor

## Platform installation steps:
The platform consists of three distinct applications and on template project in platform directory. Following steps need to be done to build the platform.

### Directory structure
Following directories have to be created:
- /usr/local/bdap
- /usr/local/bdap/projects
- /usr/local/bdap/ingestion-manager

### Ingestion Manager (IngestionManager)
#### Build
Following command creates jar file with source code that needs to be copied to: /usr/local/bdap/ingestion-manager/ing.jar
> mvn clean package
#### Run
Following command would densify the dataset 8 times using multiplication densification method and then initates ingestion to Kafka topic consumptions with the speed of 200 000 records per second.
> java -jar ing.jar -dc 8 -dt 1 -p /datasets/datasetFull.csv -rps 200000 -t consumptions

### React front-end application (bdap-frontend)
#### Build
> npm config set unsafe-perm=true

> npm install
#### Run
Front-end will be running on localhost:3000.
> npm start

### Template Flink project (BDAP-template)
It is necessary to copy this project to following directory:
- /usr/local/bdap/BDAP-template

### Big data analysis platform (BigDataAnalysisPlatform)
#### Build
> mvn clean package
#### Run
> mvn spring-boot:run

## Anomaly detection algorithms compilation and submission:
### Spark
#### Build
> mvn clean package
#### Submission example
> spark-submit --num-executors 2 --executor-cores 3 --master spark://node-master:7077 --deploy-mode cluster --class streaming.ModelBasedAnomalyDetection spark.jar 40

### Flink
#### Build
> mvn clean package
#### Submission example
> ./bin/flink run -c streaming.ModelBasedAnomalyDetection flink.jar


### Storm
#### Build
> mvn clean package
#### Submission example
> ./bin/storm jar storm.jar streaming.ModelBasedAnomalyDetection

## Environment setup
Some of these applications depend on environment setup such as programming languages installed and environment variables.
### Environment variables
Here you can see the snipped from our .bashrc file that exports these variables. Hostname node-master has to be specified in /etc/hosts with the IP address of master node.
- export SPARK_IP_PORT="node-master:7077"
- export HDFS_IP_PORT="node-master:9000"
- export WEB_HDFS_IP_PORT="node-master:50070"
- export KAFKA_IP_PORT="node-master:9092"
- export ZOOKEEPER_IP_PORT="node-master:2181"

### Programming languages installed:
- Scala - version 2.12
- Java - version 8

