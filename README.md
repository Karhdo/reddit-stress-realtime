## Project Structure

```
reddit-stress-streaming/
├── Makefile
├── README.md
├── airflow
│   └── dags
│       └── retrain_model_dag.py
├── configs
│   └── config.yaml
├── docker-compose.yml
├── requirements.txt
├── spark-warehouse
├── src
│   ├── common
│   │   ├── __init__.py
│   │   ├── config.py
│   │   ├── config_types
│   │   │   ├── __init__.py
│   │   │   ├── app_config.py
│   │   │   ├── kafka_config.py
│   │   │   ├── minio_config.py
│   │   │   ├── model_config.py
│   │   │   ├── reddit_config.py
│   │   │   ├── sink_config.py
│   │   │   └── spark_config.py
│   │   ├── logging_utils.py
│   │   └── schema.py
│   ├── model
│   │   ├── __init__.py
│   │   ├── artifacts
│   │   ├── infer.py
│   │   └── train.py
│   ├── producer
│   │   ├── __init__.py
│   │   └── reddit_producer.py
│   └── streaming
│       ├── __init__.py
│       ├── app.py
│       ├── app_backup.py
│       ├── stream_bronze.py
│       ├── stream_gold.py
│       └── stream_silver.py
└── tests
    ├── __init__.py
    └── test_infer.py
```
