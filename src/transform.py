from pyspark.sql.functions import col, avg, min, max, stddev, month, year

def transform(df):

    # limpiar datos
    df = df.filter(col("rate") > 0)

    # agregar columnas de año y mes (para agrupar)
    df = df.withColumn("year", year(col("date")))
    df = df.withColumn("month", month(col("date")))

    # 🔥 aquí usamos groupBy (más fácil de entender)
    df_grouped = df.groupBy("currency", "year", "month").agg(
        avg("rate").alias("avg_rate"),
        min("rate").alias("min_rate"),
        max("rate").alias("max_rate"),
        stddev("rate").alias("volatility")
    )

    return df_grouped