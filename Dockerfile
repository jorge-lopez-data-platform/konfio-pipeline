FROM apache/spark:3.5.0

USER root

RUN pip install requests

WORKDIR /app
COPY . /app

CMD ["spark-submit", "src/main.py"]