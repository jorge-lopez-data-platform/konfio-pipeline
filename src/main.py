from pyspark.sql import SparkSession
from extract import extract_data

def main():
    spark = SparkSession.builder.appName("KonfioPipeline").getOrCreate()

    # Extraer datos
    rows = extract_data()

    # Crear DataFrame
    df = spark.createDataFrame(rows, ["date", "currency", "rate"])

    df.show()

if __name__ == "__main__":
    main()