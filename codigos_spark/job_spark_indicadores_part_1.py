from pyspark.sql import functions as f

viagens = (
    spark
    .read
    .csv("s3://puc-trabalho-final-landing-zone-104346215011/train.csv", sep=',', header=True, inferSchema=True)
)


viagens = (
    viagens
    .select(f.col('Gender').alias('sexo'),
            f.col('Type of Travel').alias('tipo_viagem'),
            f.col('Class').alias('classe'),
            f.col('Ease of Online booking').alias('satisfacao_reserva_online'),
            f.col('Seat comfort').alias('satisfacao_conforto_assento'),
            f.col('On-board service').alias('satisfacao_servico_bordo'))
)


viagens.show()


df_ind_1 = (
    viagens
    .groupBy('sexo', 'tipo_viagem', 'classe')
    .agg(
        f.round(f.mean("satisfacao_reserva_online"), 2).alias(
            "media_satisfacao_reserva_online")
    )
    .orderBy('sexo')
)


df_ind_2 = (
    viagens
    .groupBy('sexo', 'tipo_viagem', 'classe')
    .agg(
        f.round(f.mean("satisfacao_conforto_assento"), 2).alias(
            "media_satisfacao_conforto_assento")
    )
    .orderBy('sexo')
)


df_ind_3 = (
    viagens
    .groupBy('sexo', 'tipo_viagem', 'classe')
    .agg(
        f.round(f.mean("satisfacao_servico_bordo"), 2).alias(
            "media_satisfacao_servico_bordo")
    )
    .orderBy('sexo')
)


(
    df_ind_1
    .write
    .format('parquet')
    .save('s3://puc-trabalho-final-processing-zone-104346215011/indicadores/indicador1')
)

(
    df_ind_2
    .write
    .format('parquet')
    .save('s3://puc-trabalho-final-processing-zone-104346215011/indicadores/indicador2')
)

(
    df_ind_3
    .write
    .format('parquet')
    .save('s3://puc-trabalho-final-processing-zone-104346215011/indicadores/indicador3')
)
