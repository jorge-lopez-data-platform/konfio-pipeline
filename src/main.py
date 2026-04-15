from pyspark.sql import SparkSession
from extract import extract_data
from transform import transform

def main():
    spark = SparkSession.builder.getOrCreate()

    data = extract_data()

    df = spark.createDataFrame(data, ["date", "currency", "rate"])

    df = transform(df)

    df.show()

if __name__ == "__main__":
    main()