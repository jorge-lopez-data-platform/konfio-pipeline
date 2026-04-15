def save_tables(df_detail, df_summary):
    
    df_detail.writeTo("local.db.tipos_cambio").createOrReplace()
    df_summary.writeTo("local.db.metricas_mensuales").createOrReplace()