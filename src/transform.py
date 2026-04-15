from pyspark.sql.functions import col, avg, stddev, min, max, month, year

def transform(df):

    # -----------------------------------
    # 1. LIMPIEZA DE DATOS
    # -----------------------------------

    # filtramos registros donde el rate sea inválido (por ejemplo 0 o negativo)
    # esto evita problemas en cálculos posteriores
    df = df.filter(col("rate") > 0)


    # -----------------------------------
    # 2. PREPARAR COLUMNAS PARA AGRUPAR
    # -----------------------------------

    # extraemos el año de la fecha
    # esto nos servirá para hacer agregaciones por periodo
    df = df.withColumn("year", year(col("date")))

    # extraemos el mes de la fecha
    # así podremos agrupar por mes
    df = df.withColumn("month", month(col("date")))


    # -----------------------------------
    # 3. AGREGACIONES (RESUMEN MENSUAL)
    # -----------------------------------

    # agrupamos por moneda, año y mes
    # esto genera un resumen por cada combinación
    df_summary = df.groupBy("currency", "year", "month").agg(

        # promedio del tipo de cambio en el mes
        avg("rate").alias("avg_rate"),

        # valor mínimo del mes
        min("rate").alias("min_rate"),

        # valor máximo del mes
        max("rate").alias("max_rate"),

        # volatilidad (desviación estándar)
        # indica qué tanto varió el tipo de cambio en ese mes
        stddev("rate").alias("volatility")
    )


    # -----------------------------------
    # 4. UNIR DATOS DETALLADOS + RESUMEN
    # -----------------------------------

    # hacemos un join entre los datos originales y el resumen mensual
    # esto permite tener en cada fila tanto el dato diario como el contexto del mes
    df_joined = df.join(
        df_summary,
        on=["currency", "year", "month"],  # claves para unir
        how="left"  # left join para no perder registros originales
    )


    # -----------------------------------
    # 5. DETECCIÓN DE ANOMALÍAS
    # -----------------------------------

    # marcamos como anomalía cuando el valor del día
    # es mucho mayor que el promedio del mes
    # usamos 2 * volatilidad como referencia
    df_joined = df_joined.withColumn(
        "is_anomaly",
        col("rate") > (col("avg_rate") + 2 * col("volatility"))
    )


    # -----------------------------------
    # 6. RESULTADO FINAL
    # -----------------------------------

    # df_joined → datos detallados + métricas + anomalías
    # df_summary → resumen mensual por moneda
    return df_joined, df_summary