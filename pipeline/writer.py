from pyspark.sql import DataFrame

class Writter:
    """
    Classe responsavel por persistir os dados nas camadas.
    """
    def __init__(self, logger: object):
        self.logger = logger

    def write_parquet_file(self, df: DataFrame, target_folder: str ,layer: str, partition_column: str) -> None:
        """
        Persiste um dataframe em formato Parquet em um diretorio.
        ---
        :param df: DataFrame a ser persistido
        :param target_folder: Pasta onde o arquivo Parquet será salvo
        :param layer: Nome da camada de dados (ex: 'raw', 'silver', 'gold')
        :param partition_column: Coluna pela qual o DataFrame será particionado
        """
        output_path = f'{target_folder}/{layer}'
        try:
            self.logger.info(f"Escrevendo o DataFrame na camada {layer} no diretorio: {output_path}.")
            df.write.mode('overwrite')\
                    .partitionBy(partition_column)\
                    .parquet(output_path)
            self.logger.info(f"DataFrame persistido com sucesso!")
        except Exception as e:
            self.logger.error(f"Erro ao escrever o arquivo Parquet na camada {layer}: {e}")
            raise e
