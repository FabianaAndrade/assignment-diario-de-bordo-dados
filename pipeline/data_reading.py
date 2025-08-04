from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
import os

class DataReading:
    """
    Classe responsável por realizar a leitura de arquivos 
    persistidos em CSV e Parquet.
    """
    def __init__(self, spark: SparkSession, logger: object):
        self.spark = spark
        self.logger = logger

    def read_csv_file(self, source_folder: str, sep: str = ';') -> DataFrame:
        """
        Lê um arquivo CSV de uma pasta com os dados origem para o pipeline.
        ---
        :param source_folder: Pasta onde os arquivos CSV estão armazenados
        :param sep: Separador utilizado no arquivo CSV
        :return: DataFrame contendo os dados lidos do arquivo CSV
        """
        try:
            self.logger.info(f"Iniciando leitura do arquivo CSV da pasta: {source_folder}")
            csv_files = [f for f in os.listdir(source_folder) if f.endswith('.csv')]
            full_path_csv = os.path.join(source_folder, csv_files[0])
            df_csv = self.spark.read.csv(full_path_csv, header=True, inferSchema=True, sep = sep)
            self.logger.info(f"Arquivo CSV lido com sucesso: {full_path_csv}")
            return df_csv
        except Exception as e:
            self.logger.error(f"Erro ao ler o arquivo CSV: {e}")
            raise e
        
    
    def read_parquet_file(self, layer: str, source_folder: str) -> DataFrame:
        """
        Lê um arquivo Parquet de uma camada de dados
        ---
        :param layer: Nome da camada de dados (ex: 'raw', 'silver', 'gold')
        :param source_folder: Pasta onde os arquivos Parquet estão armazenados
        :return: DataFrame contendo os dados lidos do arquivo Parquet
        """
        folder_name = f"{source_folder}/{layer}"
        self.logger.info(f"Iniciando leitura dos arquivos parquet da camada: {layer}")
        try:
            df_parquet = self.spark.read.parquet(folder_name)
            self.logger.info(f"Arquivos Parquet lidos com sucesso da camada: {layer}")
            return df_parquet
        except Exception as e:
            self.logger.error(f"Erro ao ler o arquivo Parquet: {e}")
            raise e
        
