from pyspark.sql.functions import col, concat_ws, current_timestamp


def generate_events(df):

    # -----------------------------------
    # 1. IDEA GENERAL
    # -----------------------------------
    # este script genera eventos a partir de los datos procesados
    # la idea es simular algo tipo Kafka, pero guardándolo en JSON
    # cada fila del dataframe se convierte en un evento

    
    # -----------------------------------
    # 2. CREAR ID DEL EVENTO
    # -----------------------------------
    # usamos date + currency como identificador único
    # esto nos ayuda a identificar cada registro
    df_events = df.withColumn(
        "entity_id",
        concat_ws("_", col("date"), col("currency"))
    )


    # -----------------------------------
    # 3. AGREGAR TIMESTAMP DEL EVENTO
    # -----------------------------------
    # esto indica cuándo se generó el evento
    df_events = df_events.withColumn(
        "event_timestamp",
        current_timestamp()
    )


    # -----------------------------------
    # 4. DEFINIR TIPO DE EVENTO
    # -----------------------------------
    # aquí simulamos el tipo de operación (INSERT / UPDATE)
    # en un caso real esto vendría directamente del CDC

    # lógica simple:
    # si el registro tiene updated_at → lo consideramos UPDATE
    # si no → INSERT
    df_events = df_events.withColumn(
        "event_type",
        col("operation_type")  # viene del CDC si lo implementaste
    )


    # -----------------------------------
    # 5. ARMAR EL PAYLOAD
    # -----------------------------------
    # aquí seleccionamos la información relevante del evento
    # esto sería lo que enviarías a Kafka en un sistema real
    df_events = df_events.select(
        "event_type",
        "event_timestamp",
        "entity_id",
        "date",
        "currency",
        "rate",
        "year",
        "month"
    )


    # -----------------------------------
    # 6. GUARDAR EVENTOS
    # -----------------------------------
    # guardamos en formato JSON
    # esto simula una salida tipo streaming
    df_events.write \
        .mode("overwrite") \
        .json("/app/events/")


    # mensaje simple para confirmar
    print("eventos generados")