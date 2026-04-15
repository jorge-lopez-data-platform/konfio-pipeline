from pyspark.sql.functions import current_timestamp, lit

def apply_cdc(spark, df):



    #agregar nuevos campos de cambios
    
     # timestamp de cuando se procesa el dato
    df = df.withColumn("ingestion_timestamp", current_timestamp())

    # este campo se usa cuando hay updates
    df = df.withColumn("updated_at", current_timestamp())

    # por default marcamos como INSERT (luego el merge se encarga de actualizar)
    df = df.withColumn("operation_type", lit("INSERT"))


    # crear la bd si no existe
    spark.sql("CREATE DATABASE IF NOT EXISTS local.db")

    # guardar los datos nuevos como una "Tabla temporal"

    df.createOrReplaceTempView("tmp_exchange_rates")

    # crear la tabla o remplazar si no existe
    # aquí es donde se guardan los datos finales
    spark.sql("""
    CREATE TABLE IF NOT EXISTS local.db.exchange_rates (
        date DATE,
        currency STRING,
        rate DOUBLE,
        year INT,
        month INT,
        ingestion_timestamp TIMESTAMP,
        updated_at TIMESTAMP
    )
    USING iceberg
    PARTITIONED BY (year, month)
    """)

    # aquí hacemos la comparación entre lo nuevo y lo que ya existe
    spark.sql("""
    MERGE INTO local.db.exchange_rates AS A
    USING tmp_exchange_rates AS B
    ON A.date = B.date AND A.currency = B.currency

    -- si ya existe y cambió el valor → UPDATE
    WHEN MATCHED AND A.rate != B.rate THEN
        UPDATE SET
            A.rate = B.rate,
            A.year = B.year,
            A.month = B.month,
            A.updated_at = current_timestamp()

    -- si no existe → INSERT
    WHEN NOT MATCHED THEN
        INSERT (
            date, currency, rate, year, month,
            ingestion_timestamp, updated_at
        )
        VALUES (
            B.date, B.currency, B.rate, B.year, B.month,
            B.ingestion_timestamp, B.updated_at
        )
    """)

 #crear dataset para los nuevos eventos

    df_events = df.withColumn(
        "operation_type",
        lit("INSERT")  # agrega el comentario de insertado
    )

    print("cdc aplicado correctamente")

    return df_events


#Se usa date + currency como llave
#Si cambia el rate → se actualiza
#Si es nuevo → se inserta

#si corre Docker
# sicorre Spark
# el MERGE si funciona
# ya escribe en Iceberg 
# ya corre pipeline completo