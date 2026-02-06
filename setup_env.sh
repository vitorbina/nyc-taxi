#!/bin/bash
export AIRFLOW_HOME=$(pwd)/airflow
echo "AIRFLOW_HOME definido para: $AIRFLOW_HOME"
pip install -r requirements.txt
airflow db init
