def apply_cdc(spark, df):

    # guardar los datos nuevos como una "Tabla temporal"

    df.createOrReplaceTempView("tmp_exchange_rates")

    # crear la tabla o remplazar si no existe
    # aquí es donde se guardan los datos finales
    spark.sql("""
    CREATE OR REPLACE TABLE IF NOT EXISTS local.db.exchange_rates (
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

    -- si ya existía pero cambió el valor → actualiza
    WHEN MATCHED AND A.rate != B.rate THEN
        UPDATE SET *

    -- si no existe , inserta
    WHEN NOT MATCHED THEN
        INSERT *
    """)