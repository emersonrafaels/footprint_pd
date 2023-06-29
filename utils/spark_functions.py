# IMPORTING LIBRARIES
import os
import typing
import logging
from logging.config import fileConfig
from inspect import stack

import pyspark.sql
from pyspark.sql import SparkSession
from pyspark.sql.types import *

# SETTING ENVIRONMENT VARIABLES - SPARK
os.environ["PYSPARK_DRIVER_PYTHON"] = r"C:\Users\Emerson\SPARK\Scripts\python.exe"
os.environ["PYSPARK_PYTHON"] = r"C:\Users\Emerson\SPARK\Scripts\python.exe"

# CREATING SPARK SESSION
spark = SparkSession.builder.appName("LESSON_PYSPARK_DATAFRAME").getOrCreate()

# INICIALIZANDO O LOGGER
fileConfig('logging_config.ini')
logger = logging.getLogger()


def spark_read_csv(data_dir: typing.Union[str, os.PathLike],
                   header: bool = True,
                   sep: str = ",",
                   encoding: str = "latin1",
                   schema=None):

    """

    FUNÇÃO PARA REALIZAR LEITURA DE DADOS
    DE UM CSV
    USANDO SPARK DATAFRAME

    # Arguments
        data_dir                    - Required: Diretório do arquivo excel a ser lido (Path)
        header                      - Optional: Se os dados possuem cabeçalho (Boolean)
        sep                         - Optional: Separador dos dados (String)
        encoding                    - Optional: Qual o formato de codificação dos dados.
        schema                      - Optional: Tipagem de cada coluna. É um dict,
                                                no qual a chave representa o
                                                nome da coluna e o
                                                valor seu tipo, respectivamente.

    # Returns
        df                          - Required: Dados obtidos (Spark DataFrame)

    """

    # INICIALIZANDO DATAFRAME VAZIO
    df_csv = spark.DataFrame()

    logger.info("INICIANDO A LEITURA DOS DADOS UTILIZANDO SPARK CSV")

    try:
        # VERIFICANDO SE UM SCHEMA FOI DEFINIDO
        if schema is None:

            logger.info("REALIZANDO LEITURA SEM SCHEMA DEFINIDO")

            df_csv = (
                spark.read.option("delimiter", sep)
                    .option("header", header)
                    .option("encoding", encoding)
                    .csv(data_dir)
            )

        elif isinstance(schema, [StructType, str]):

            logger.info("REALIZANDO LEITURA COM SCHEMA DEFINIDO: {}".format(schema))

            df_csv = (
                spark.read.option("delimiter", sep)
                .option("header", header)
                .option("encoding", encoding)
                .schema(schema)
                .csv(data_dir)
            )

        logger.info("LEITURA REALIZADA COM SUCESSO")

    except Exception as ex:
        logger.error("ERRO NA FUNÇÃO: {} - {}".format(stack()[0][3], ex))

    return df_csv


def spark_read_parquet(data_dir: typing.Union[str, os.PathLike],
                       header: bool = True,
                       schema=None):

    """

    FUNÇÃO PARA REALIZAR LEITURA DE DADOS
    DE UM PARQUET
    USANDO SPARK DATAFRAME

    # Arguments
        data_dir                    - Required: Diretório do arquivo excel a ser lido (Path)
        header                      - Optional: Se os dados possuem cabeçalho (Boolean)
        schema                      - Optional: Tipagem de cada coluna. É um dict,
                                                no qual a chave representa o
                                                nome da coluna e o
                                                valor seu tipo, respectivamente.

    # Returns
        df                          - Required: Dados obtidos (Spark DataFrame)

    """

    # INICIALIZANDO DATAFRAME VAZIO
    df_parquet = spark.DataFrame()

    logger.info("INICIANDO A LEITURA DOS DADOS UTILIZANDO SPARK PARQUET")

    try:
        # VERIFICANDO SE UM SCHEMA FOI DEFINIDO
        if schema is None:

            logger.info("REALIZANDO LEITURA SEM SCHEMA DEFINIDO")

            df_parquet = (
                spark.read.option("header", header)
                    .parquet(data_dir)
            )

        elif isinstance(schema, [StructType, str]):

            logger.info("REALIZANDO LEITURA COM SCHEMA DEFINIDO: {}".format(schema))

            df_parquet = (
                spark.read.option("header", header)
                    .schema(schema)
                    .parquet(data_dir)
            )

        logger.info("LEITURA REALIZADA COM SUCESSO")

    except Exception as ex:
        logger.error("ERRO NA FUNÇÃO: {} - {}".format(stack()[0][3], ex))

    return df_parquet

def spark_read_parquet_last_partition(data_dir: typing.Union[str, os.PathLike],
                                      header: bool = True,
                                      schema=None):

    """

    FUNÇÃO PARA REALIZAR LEITURA DE DADOS
    DE UM PARQUET PARTICIONANDO
    USANDO SPARK DATAFRAME

    RETORNA A ÚLTIMA PARTIÇÃO

    # Arguments
        data_dir                    - Required: Diretório do arquivo excel a ser lido (Path)
        header                      - Optional: Se os dados possuem cabeçalho (Boolean)
        schema                      - Optional: Tipagem de cada coluna. É um dict,
                                                no qual a chave representa o
                                                nome da coluna e o
                                                valor seu tipo, respectivamente.

    # Returns
        df                          - Required: Dados obtidos (Spark DataFrame)

    """

    # INICIALIZANDO DATAFRAME VAZIO
    df_parquet = spark.DataFrame()

    logger.info("INICIANDO A LEITURA DOS DADOS UTILIZANDO SPARK PARQUET")

    try:

        # OBTÉM A LISTA DE DIRETÓRIOS DE PARTIÇÃO
        dir_partitions = spark._jvm.org.apache.hadoop.fs.Path(data_dir) \
            .getFileSystem(spark._jsc.hadoopConfiguration()) \
            .listStatus(spark._jvm.org.apache.hadoop.fs.Path(data_dir))

        logger.info("DIRETÓRIOS PARTICIONADOS: {}".format(dir_partitions))

        # ORDENANDO OS DIRETÓRIOS DE PARTIÇÃO PELA ORDEM LEXICOGRÁFICA REVERSA
        dir_partitions = sorted(dir_partitions, key=lambda x: x.getPath().toString(), reverse=True)

        # OBTÉM O PATH PARA A ÚLTIMA PARTIÇÃO
        dir_last_partition = dir_partitions[0].getPath().toString()

        logger.info("DIRETÓRIO DE ÚLTIMA PARTIÇÃO: {}".format(dir_last_partition))

        # VERIFICANDO SE UM SCHEMA FOI DEFINIDO
        if schema is None:

            logger.info("REALIZANDO LEITURA SEM SCHEMA DEFINIDO")

            df_parquet = (
                spark.read.option("header", header)
                    .parquet(dir_last_partition)
            )

        elif isinstance(schema, [StructType, str]):

            logger.info("REALIZANDO LEITURA COM SCHEMA DEFINIDO: {}".format(schema))

            df_parquet = (
                spark.read.option("header", header)
                    .schema(schema)
                    .parquet(dir_last_partition)
            )

        logger.info("LEITURA REALIZADA COM SUCESSO")

    except Exception as ex:
        logger.error("ERRO NA FUNÇÃO: {} - {}".format(stack()[0][3], ex))

    return df_parquet