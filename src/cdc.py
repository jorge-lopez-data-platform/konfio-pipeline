def apply_cdc(spark, df):

    # guardar los datos nuevos como una "Tabla temporal"

    df.createOrReplaceTempView("tmp_exchange_rates")

    # crear la bd si no existe
    spark.sql("CREATE DATABASE IF NOT EXISTS local.db")

    # crear la tabla o remplazar si no existe
    # aquí es donde se guardan los datos finales
    spark.sql("""
    CREATE TABLE IF NOT EXISTS local.db.exchange_rates (
        date DATE,
        currency STRING,
        rate DOUBLE,
        year INT,
        month INT
    )
    USING iceberg
    PARTITIONED BY (year, month)
    """)

    # aquí hacemos la comparación entre lo nuevo y lo que ya existe
    spark.sql("""
    MERGE INTO local.db.exchange_rates AS A
    USING tmp_exchange_rates AS B
    ON A.date = B.date AND A.currency = B.currency

    -- si ya existe y cambió → actualizar columnas
    WHEN MATCHED AND A.rate != B.rate THEN
        UPDATE SET
            A.date = B.date,
            A.currency = B.currency,
            A.rate = B.rate,
            A.year = B.year,
            A.month = B.month

    -- si no existe → insertar
    WHEN NOT MATCHED THEN
        INSERT (date, currency, rate, year, month)
        VALUES (B.date, B.currency, B.rate, B.year, B.month)
    """)