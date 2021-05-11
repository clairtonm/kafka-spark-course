# Getting Started

Apache Kafka + Elastic Stack + Apache Superset on Docker.

Based on docker files of oficial repos of Elastic and Apache Superset

## Prerequisites

1. Docker! [link](https://www.docker.com/get-started)
1. Docker-compose [link](https://docs.docker.com/compose/install/)

## Normal Operation

To run the container, simply run:

```bash
docker-compose up -d
```

In manual_ingestion directory there are the notebook to get the data from tweets.json and ingest on Kafka or directly on Elasticsearch. Logstash will running using the configuration on logstash/logstash.conf and will connect on kafka topic to ingest on Elasticsearch as well.

## Links

- **Apache Superset:** [http://localhost:8088](http://localhost:8088)
- **Elasticsearch:** [http://localhost:9200](http://localhost:9200)
- **Kibana:** [http://localhost:5601](http://localhost:5601)

## Create Env

`conda create --name myenv`

Jupyter and PySpark: https://www.sicara.ai/blog/2017-05-02-get-started-pyspark-jupyter-notebook-3-minutes