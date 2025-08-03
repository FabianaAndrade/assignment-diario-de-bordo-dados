from pyspark.sql import functions as f
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession

class DataTransformation:
    """
    Classe responsavel por realizar operacoes de transformacoes
    nas bases de dados.
    """
    def __init__(self, spark: SparkSession, logger: object):
        self.spark = spark
        self.logger = logger

    def date_ref_column(self, df: DataFrame, column_ref: str) -> DataFrame:
        """
        Metodo para formatar a coluna de data de referencia
        ---
        :param df: DataFrame a ser transformado
        :param column_ref: Nome da coluna que possui a data de ref
        :return: DataFrame com a coluna de data de referencia formatada
        """
        try:
            self.logger.info(f"Formatando a coluna de data de referencia: '{column_ref}' para 'yyyy-MM-dd' format.")
            df = df.withColumn(
                    'DT_REFE',
                    f.to_date(f.to_timestamp(f.col(column_ref), 
                    'MM-dd-yyyy H:mm'), 
                    'yyyy-MM-dd')
                )
        except Exception as e:
            print(f"Erro ao formatar a coluna de data de referencia: {e}")
            raise e
        return df
    
    def change_data_type(self, df: DataFrame, column_name: str, new_data_type: str) -> DataFrame:
        """
        Realiza a mudança do tipo de dado de uma coluna especificada
        ---
        :param df: DataFrame a ser transformado
        :param column_name: Nome da coluna cujo tipo de dado será alterado
        :param new_data_type: Novo tipo de dado para a coluna
        :return: DataFrame com a coluna alterada para o novo tipo de dado
        """
        self.logger.info(f"Alterando o tipo de dado da coluna '{column_name}' para '{new_data_type}'.")
        try:
            df = df.withColumn(column_name, f.col(column_name).cast(new_data_type))
        except Exception as e:
            self.logger.error(f"Erro ao alterar o tipo de dado da coluna '{column_name}': {e}")
            raise e
        return df
    
    def replace_missing_values(self, df: DataFrame, columns_names: list, value: str = 'não_registrado') -> DataFrame:
        """
        Substitui valores nulos ou strings específicas em colunas especificadas por um valor padrão.
        ---
        :param df: DataFrame a ser transformado
        :param columns_names: Lista de nomes das colunas onde os valores serão substituídos
        :param value: Valor padrão para substituir valores nulos ou strings específicas
        :return: DataFrame com os valores substituídos
        """
        for column in columns_names:
            self.logger.info(f"Substituindo valores faltantes da '{column}' por '{value}'.")
            try: 
                df = df.withColumn(
                        column,
                        f.when(
                            f.col(column).isNull() | f.lower(f.col(column)).isin(['unknown location', 'unk', 'n/a']),
                            value
                        ).otherwise(f.col(column))
                    )
            except Exception as e:
                self.logger.error(f"Erro ao substituir valores na coluna '{column}': {e}")
                raise e
        return df
