from pyspark.sql.functions import col, avg, stddev, min, max, month, year

def transform(df):

    # limpiar datos (evitar valores inválidos)
    df = df.filter(col("rate") > 0)

    # agregar columnas de año y mes para agrupar
    df = df.withColumn("year", year(col("date")))
    df = df.withColumn("month", month(col("date")))

    # resumen por moneda y mes
    df_summary = df.groupBy("currency", "year", "month").agg(
        avg("rate").alias("avg_rate"),
        min("rate").alias("min_rate"),
        max("rate").alias("max_rate"),
        stddev("rate").alias("volatility")
    )

    # detección simple de anomalías:
    # si el valor se aleja mucho del promedio mensual
    df_joined = df.join(
        df_summary,
        on=["currency", "year", "month"],
        how="left"
    )

    df_joined = df_joined.withColumn(
        "is_anomaly",
        col("rate") > (col("avg_rate") + 2 * col("volatility"))
    )

    return df_joined, df_summary