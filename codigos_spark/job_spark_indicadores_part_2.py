from pyspark.sql import functions as f

df_indi_result_1 = (
    spark
    .read
    .parquet("s3://puc-trabalho-final-processing-zone-104346215011/indicadores/indicador1")
)

df_indi_result_2 = (
    spark
    .read
    .parquet("s3://puc-trabalho-final-processing-zone-104346215011/indicadores/indicador2")
)

df_indi_result_3 = (
    spark
    .read
    .parquet("s3://puc-trabalho-final-processing-zone-104346215011/indicadores/indicador3")
)

df_final = (
    df_indi_result_1
    .join(df_indi_result_2, how='inner', on=['sexo', 'tipo_viagem', 'classe'])
)

df_final = (
    df_final
    .join(df_indi_result_3, how='inner', on=['sexo', 'tipo_viagem', 'classe'])
)

(
    df_final
    .write
    .format('parquet')
    .save('s3://puc-trabalho-final-processing-zone-104346215011/indicador-final/')
)
