import os
import sys

# Define onde está o Python e o JAVA deste ambiente específico
# Substitua 'vitor' pelo seu usuário se for diferente
CONDA_PREFIX = "/home/vitor/miniconda3/envs/projeto-dados"

os.environ['JAVA_HOME'] = CONDA_PREFIX
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# Remove variáveis que podem puxar o Spark/Java do trabalho
os.environ.pop('SPARK_HOME', None)

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("Estudo_NYC_Vitor") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://127.0.0.1:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262") \
    .getOrCreate()

print("\n🚀 ISOLAMENTO COMPLETO: Spark rodando com Java 17 do Conda!")