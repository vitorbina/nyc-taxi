#!/usr/bin/env bash
set -e

echo "==> [1/3] Downloading Hive JDBC driver..."
mkdir -p hive-lib
wget -q "https://jdbc.postgresql.org/download/postgresql-42.7.3.jar" -O hive-lib/postgresql.jar
echo "    Done: hive-lib/postgresql.jar"

echo "==> [2/3] Creating Hive S3A config..."
mkdir -p hive-conf
cat > hive-conf/core-site.xml << 'EOF'
<?xml version="1.0"?>
<configuration>
  <property>
    <name>fs.s3a.endpoint</name>
    <value>http://minio:9000</value>
  </property>
  <property>
    <name>fs.s3a.access.key</name>
    <value>minioadmin</value>
  </property>
  <property>
    <name>fs.s3a.secret.key</name>
    <value>minioadmin</value>
  </property>
  <property>
    <name>fs.s3a.path.style.access</name>
    <value>true</value>
  </property>
  <property>
    <name>fs.s3a.impl</name>
    <value>org.apache.hadoop.fs.s3a.S3AFileSystem</value>
  </property>
  <property>
    <name>fs.s3a.aws.credentials.provider</name>
    <value>org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider</value>
  </property>
  <property>
    <name>fs.s3a.connection.ssl.enabled</name>
    <value>false</value>
  </property>
</configuration>
EOF
echo "    Done: hive-conf/core-site.xml"

echo "==> [3/3] Creating MinIO bucket..."
docker compose up -d minio
echo "    Waiting for MinIO to be ready..."
until curl -sf http://localhost:9000/minio/health/live > /dev/null 2>&1; do
    sleep 2
done
docker run --rm --network nyc-taxi_nyc-network \
    minio/mc:latest \
    sh -c "mc alias set local http://minio:9000 minioadmin minioadmin && mc mb --ignore-existing local/data-lake-nyc"
echo "    Done: bucket data-lake-nyc created"

echo ""
echo "Setup complete. Run 'docker compose up -d' to start the full infrastructure."
