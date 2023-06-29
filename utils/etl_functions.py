# IMPORTING LIBRARIES
import os
import typing

import pandas as pd
import pyspark.sql
from pyspark.sql import SparkSession
from pyspark.sql.types import *

from utils import pandas_functions
from utils import spark_functions

# SETTING ENVIRONMENT VARIABLES - SPARK
os.environ["PYSPARK_DRIVER_PYTHON"] = r"C:\Users\Emerson\SPARK\Scripts\python.exe"
os.environ["PYSPARK_PYTHON"] = r"C:\Users\Emerson\SPARK\Scripts\python.exe"



class Read_Files():

    def __init__(self):

        pass

    @staticmethod
    def show_dataframe(df, num_rows_to_display=5):

        """

            FUNÇÃO PARA VISUALIZAR UM DATAFRAME
            FUNCIONA PARA DATAFRAMES PANDAS
            E DATAFRAMES SPARK

            TAMBÉM MOSTRA O TIPAGEM DOS DADOS

            # Arguments
                df                     - Required: Dataframe a ser visualizado (DataFrame)
                num_rows_to_display    - Optional: Quantidade de linhas
                                                   para visualizar (Integer)

        """

        # VERIFICANDO SE O DATAFRAME É PANDAS
        if isinstance(df, pd.DataFrame):

            # DISPLAY CABEÇALHO DOS DADOS
            print(df.head(num_rows_to_display))

            # DISPLAY TIPAGEM DOS DADOS
            print(df.dtypes)

        # VERIFICANDO SE O DATAFRAME É PANDAS
        elif isinstance(df, pyspark.sql.DataFrame):

            # DISPLAY CABEÇALHO DOS DADOS
            print(df.show(num_rows_to_display))

            # DISPLAY TIPAGEM DOS DADOS
            print(df.dtypes)


    def execute_read_file(self,
                          data_dir: typing.Union[str, os.PathLike],
                          read_pandas=False,
                          read_spark=False,
                          sheet_name: typing.Union[int, str] = 0,
                          header_spark: bool = True,
                          header_num_row: typing.Union[int, None] = 0,
                          names_cols: typing.Union[list, None] = None,
                          columns_to_use: typing.Union[list, None] = None,
                          schema: typing.Union[dict, None] = None,
                          sep: str = ",",
                          encoding: str = 'latin1'):

        if read_pandas:
            if ".csv" in data_dir:
                pandas_functions.pandas_read_csv(data_dir=data_dir,
                                                 sep=sep,
                                                 encoding=encoding,
                                                 header_num_row=header_num_row,
                                                 names_cols=names_cols,
                                                 columns_to_use=columns_to_use,
                                                 schema=schema)

        if read_spark:
            if ".csv" in data_dir:
                spark_functions.spark_read_csv(data_dir=data_dir,
                                               header=header_spark,
                                               sep=sep,
                                               encoding=encoding,
                                               schema=schema)


class Save_Files():

    def __init__(self):

        pass

