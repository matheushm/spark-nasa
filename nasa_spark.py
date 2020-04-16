#!/usr/bin/env python
# coding: utf-8

import re
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import TimestampType, IntegerType
from datetime import datetime
import pandas as pd

def create_spark_session():
    """Cria uma Seção Spark
    
    Arguments:
        None

    Returns:
        spark -- Spark Session   
    """

    spark = SparkSession.builder \
                        .appName('NASA Kennedy Space Center Data') \
                        .getOrCreate()

    return spark


def process_data(spark, data_folder):
    """Faz a leitura dos arquivos gzip de uma pasta e processa o Spark Dataframe
    
    Arguments:
        spark -- Spark Session
        data_folder -- pasta contendo os arquivos gzip

    Returns:
        Spark Dataframe   
    """
    
    # Reading files from folder
    df_nasa = spark.read.text(data_folder)

    # Regex for group matching
    parse_regex = '(.*) - - \[([\w:/]+\s[+\-]\d{4})\] \"(.*)\" (\d{3}) ([0-9]*|-)'
    
    # Create columns based on Regex group match
    df = df_nasa.withColumn('host',F.regexp_extract(F.col('value'),parse_regex,1)) \
                .withColumn('timestamp',F.regexp_extract(F.col('value'),parse_regex,2)) \
                .withColumn('request',F.regexp_extract(F.col('value'),parse_regex,3)) \
                .withColumn('status_code',F.regexp_extract(F.col('value'),parse_regex,4).cast(IntegerType())) \
                .withColumn('bytes',F.regexp_extract(F.col('value'),parse_regex,5).cast(IntegerType())) \
                .drop('value')

    return df


def run_queries(df, output_file):
    """Faz as consultas no Spark Dataframe e gera arquivo de resposta
    
    Arguments:
        df {dataframe} -- NASA Spark Dataframe
        output_file {txt file} -- Txt com as repostas das perguntas

    Returns:
        None   
    """
    # Output file
    file = open(output_file,"w+")

    ## 1. Número de hosts únicos

    unique_hosts = df.select('host') \
                    .filter((df['host'] \
                    .isNotNull()) & (df['host'] !='')) \
                    .distinct() \
                    .count()
    file.write(f'1. Número de hosts únicos: {unique_hosts} \n\n')

    ## 2. Total de erros 404
    
    df_404 = df.filter(df['status_code']==404)
    
    # Como o dataframe será usado para próximas questões, será usado persist para otimizar a leitura e forçada a ação com count
    df_404.persist().count()
    errors_404 = df_404.count()
    file.write(f'2. Total de erros 404: {errors_404} \n\n')

    ## 3. Os 5 URLs que mais causaram erro 404

    # Regex para quebrar a requisição 
    url_regex = '(.*) (.*) (.*)'

    # Criação da coluna 'url' a partir da concatenação do 'host' + 'endpoint', agrupamento, contar requisições e limitar aos 5 primeiros
    url_df = df_404.withColumn('url',F.concat(F.col('host'), F.regexp_extract(F.col('request'),url_regex,2))) \
                   .groupBy('url') \
                   .count() \
                   .orderBy(F.desc('count')) \
                   .limit(5)
    
    # Converter para Pandas Dataframe para escrever em arquivo
    url_df_pandas = url_df.toPandas()

    file.write('3. Os 5 URLs que mais causaram erro: \n\n')
    file.write(url_df_pandas['url'].to_string(header=False,index=False))
    file.write('\n\n')

    ## 4. Quantidade de erros 404 por dia

    # UDF para extrair a data
    get_date = F.udf(lambda x: datetime.strptime(x.split(':')[0], '%d/%b/%Y'),TimestampType())

    # Criação da coluna 'date', agrupamento da data, agrupamento e contagem
    daily_404 = df_404.withColumn('date', F.to_date(get_date(df_404['timestamp']))) \
                      .select('date','status_code') \
                      .groupBy('date') \
                      .agg({'status_code':'count'}) \
                      .withColumnRenamed('count(status_code)','errors_404') \
                      .orderBy('date')
    
    # Converter para Pandas Dataframe para escrever em arquivo
    daily_404_pd = daily_404.toPandas()

    file.write('4. Quantidade de erros 404 por dia: \n\n')
    file.write(daily_404_pd.to_string(index=False, justify='left', col_space=25))
    file.write('\n\n')

    ## 5. Total de bytes retornados

    # Conversão de byte para Gb
    byte_to_gb = 1073741824

    # Agregando em soma a coluna bytes
    transmitted = df.agg(F.sum("bytes")).collect()[0][0]/byte_to_gb
    file.write(f'5. Total de bytes retornados: {round(transmitted,2)} Gb')

    # Close output file
    file.close()


def main():  

    data_folder = 'nasa-data'
    out_filename = 'nasa_data_output.txt'

    spark = create_spark_session()
    df = process_data(spark=spark, data_folder=data_folder)
    run_queries(df=df, output_file=out_filename)


if __name__ == "__main__":
    main()
