sh /opt/spark/bin/spark-submit --master spark://spark-master:7077 --total-executor-cores 1 --executor-memory 512M --jars $(echo /opt/airflow/jars/*.jar | tr ' ' ',') /opt/spark-apps/streaming/read_kafka_topic.py