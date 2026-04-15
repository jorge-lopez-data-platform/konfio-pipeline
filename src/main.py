from pyspark.sql import SparkSession
from extract import extract_data
from transform import transform
from cdc import apply_cdc

def main():

    # iniciamos spark
    spark = SparkSession.builder.getOrCreate()

    # paso 1: traer datos desde la API
    data = extract_data()

    # paso 2: convertir esos datos a dataframe
    df = spark.createDataFrame(data, ["date", "currency", "rate"])

    # paso 3: aplicar transformaciones (limpieza, métricas, etc)
    df_new_data, df_summary = transform(df)

    # paso 4: aplicar CDC
    # aquí es donde evitamos duplicados y actualizamos datos
    apply_cdc(spark, df_new_data)

    # solo para confirmar que terminó
    print("se extrajeron los datos d ela api, se aplicaron cambios y se actualizaron")

if __name__ == "__main__":
    main()