# BBIM707 Assessment 2 - code and data store
Requirements: Python 3.10+, MySQL 8 (MySQL Workbench), pip install pandas matplotlib mysql-connector-python pyspark pymongo confluent-kafka

Run order
1. PowerShell: $env:MYSQL_PASSWORD="YOUR_PASSWORD"     (optional: MYSQL_HOST, MYSQL_USER, MYSQL_PORT, MYSQL_DB)
   python partA_etl_analysis.py      -> transforms the data, writes output/*_clean.csv and charts, then loads MySQL via partA_loading_mysql.py
   (loading alone can be re-run with: python partA_loading_mysql.py)
   Then run partA_mysql_workbench.sql in MySQL Workbench for the verification screenshots.
2. python partB3_feeder.py --prepare
3. python partB1_hdfs_ingest.py          (HDFS_REPLICATION=1 on a single DataNode)
4. python partB2_mongodb.py              (mongod on localhost:27017)
5. python partB3_streaming_kmeans.py train; python partB3_streaming_kmeans.py stream  (+ python partB3_feeder.py --replay 6 --delay 4)
6. bash partB4_kafka_setup.sh; python partB4_kafka_producer.py; python partB4_kafka_consumer.py --id c1
