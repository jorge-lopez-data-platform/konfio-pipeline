from pyspark.sql.functions import col, lag, avg, stddev
from pyspark.sql.window import Window

def transform(df):

    # 1. Limpiar datos
    df = df.filter(col("rate") > 0)

    # 2. Crear una "ventana" por moneda ordenada por fecha
    window = Window.partitionBy("currency").orderBy("date")

    # 3. Cambio diario
    df = df.withColumn(
        "daily_change",
        (col("rate") - lag("rate").over(window)) / lag("rate").over(window)
    )

    # 4. Promedio últimos 7 días
    window7 = window.rowsBetween(-6, 0)
    df = df.withColumn("ma7", avg("rate").over(window7))

    # 5. Promedio últimos 30 días
    window30 = window.rowsBetween(-29, 0)
    df = df.withColumn("ma30", avg("rate").over(window30))

    # 6. Volatilidad
    df = df.withColumn("volatility", stddev("rate").over(window30))

    return df