FROM apache/spark:3.5.0

USER root

# Instalar Python y pip (necesario)
RUN apt-get update && apt-get install -y python3 python3-pip

# Instalar librerías
RUN pip3 install requests

WORKDIR /app
COPY . /app

# Ejecutar con ruta completa
CMD ["/opt/spark/bin/spark-submit", "src/main.py"]