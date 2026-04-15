FROM apache/spark:3.5.0

USER root

RUN apt-get update && apt-get install -y python3 python3-pip
RUN pip3 install requests

WORKDIR /app
COPY . /app

CMD ["/opt/spark/bin/spark-submit", "--packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.2", "src/main.py"]