# IMPORTING LIBRARIES
import os
import typing
import logging
from logging.config import fileConfig
from inspect import stack

import pandas as pd


# INICIALIZANDO O LOGGER
fileConfig('logging_config.ini')
logger = logging.getLogger()


def pandas_read_csv(data_dir: typing.Union[str, os.PathLike],
                    sep: str = ",",
                    encoding: str = 'latin1',
                    header_num_row: typing.Union[int, None] = 0,
                    names_cols: typing.Union[list, None] = None,
                    columns_to_use: typing.Union[list, None] = None,
                    schema: typing.Union[dict, None] = None) -> pd.DataFrame:

    """

    FUNÇÃO PARA REALIZAR LEITURA DE DADOS
    DE UM CSV
    USANDO PANDAS DATAFRAME

    # Arguments
        data_dir                    - Required: Diretório do arquivo excel a ser lido (Path)
        sep                         - Optional: Separador dos dados (String)
        encoding                    - Optional: Qual o formato de codificação dos dados.
        header_num_row              - Optional: Número da linha a ser considera
                                                como cabeçalho dos dados (Integer)
        names_cols                  - Optional: Nome das colunas (List)
        columns_to_use              - Optional: Nome das colunas para usar (List)
        schema                      - Optional: Tipagem de cada coluna. É um dict,
                                                no qual a chave representa o
                                                nome da coluna e o
                                                valor seu tipo, respectivamente.

    # Returns
        df                          - Required: Dados obtidos (Pandas DataFrame)

    """

    # INICIALIZANDO DATAFRAME VAZIO
    df_csv = pd.DataFrame()

    logger.info("INICIANDO A LEITURA DOS DADOS UTILIZANDO PANDAS CSV")

    try:
        df_csv = pd.read_csv(
            data_dir,
            sep=sep,
            encoding=encoding,
            header='infer',
            skiprows=header_num_row,
            names=names_cols,
            usecols=columns_to_use,
            dtype=schema)

        logger.info("LEITURA REALIZADA COM SUCESSO")

    except Exception as ex:
        logger.error("ERRO NA FUNÇÃO: {} - {}".format(stack()[0][3], ex))

    return df_csv


def pandas_read_excel(data_dir: typing.Union[str, os.PathLike],
                      sheet_name: typing.Union[int, str] = 0,
                      header_num_row: typing.Union[int, None] = 0,
                      names_cols: typing.Union[list, None] = None,
                      columns_to_use: typing.Union[list, None] = None,
                      schema: typing.Union[dict, None] = None) -> pd.DataFrame:

    """

    FUNÇÃO PARA REALIZAR LEITURA DE DADOS
    DE UM EXCEL
    USANDO PANDAS DATAFRAME

    # Arguments
        data_dir                    - Required: Diretório do arquivo excel a ser lido (Path)
        sheet_name                  - Optional: Aba do arquivo excel a ser lido (String)
        header_num_row              - Optional: Número da linha a ser considera
                                                como cabeçalho dos dados (Integer)
        names_cols                  - Optional: Nome das colunas (List)
        columns_to_use              - Optional: Nome das colunas para usar (List)
        schema                      - Optional: Tipagem de cada coluna. É um dict,
                                                no qual a chave representa o
                                                nome da coluna e o
                                                valor seu tipo, respectivamente. (None | StructType)

    # Returns
        df                          - Required: Dados obtidos (Pandas DataFrame)

    """

    # INICIALIZANDO DATAFRAME VAZIO
    df = pd.DataFrame()

    logger.info("INICIANDO A LEITURA DOS DADOS UTILIZANDO PANDAS EXCEL")

    try:
        df = pd.read_excel(data_dir,
                           sheet_name=sheet_name,
                           header=header_num_row,
                           names=names_cols,
                           usecols=columns_to_use,
                           dtype=schema,
                           engine='openpyxl')

        logger.info("LEITURA REALIZADA COM SUCESSO")

    except:
        try:
            df = pd.read_excel(data_dir,
                               sheet_name=sheet_name,
                               header=header_num_row,
                               names=names_cols,
                               usecols=columns_to_use,
                               dtype=schema)

            logger.info("LEITURA REALIZADA COM SUCESSO")

        except Exception as ex:
            logger.error("ERRO NA FUNÇÃO: {} - {}".format(stack()[0][3], ex))

    return df