from pyspark.sql import functions as f
from pyspark.sql import DataFrame


class DataAggregation:
    """
    Classe responsável por realizar operações de agregação, 
    criar a base gold.
    """
    def __init__(self, logger: object):
        self.logger = logger

    def agg_corridas_dia(self, df_silver: DataFrame) -> DataFrame:
        """
        Cria a base de dados agregada com as corridas por dia.
        ---
        :param df_silver: DataFrame contendo os dados das corridas
        :return: DataFrame agregado (gold) com as corridas por dia
        """
        self.logger.info("Iniciando agregação de dados para corridas por dia.")
        self.logger.info("""Regra de negocio:
                         - Contagem total de corridas por dia
                         - Contagem de corridas por categoria (Negocio, Pessoal)
                         - Distância máxima, mínima e média por dia
                         - Contagem de corridas com propósito 'Reunião'
                         - Contagem de corridas com propósito diferente de 'Reunião'
                         - resultado agrupado por Data de referência (DT_REFE)
                         """)
        try:
            agg_rules = [
                f.count("*").alias("QT_CORR"),
                f.count(
                    f.when(f.lower(f.col("CATEGORIA")) == "negocio", True)
                ).alias("QT_CORR_NEG"),
                f.count(
                    f.when(f.lower(f.col("CATEGORIA")) == "pessoal", True)
                ).alias("QT_CORR_PESS"),
                f.round(f.max("DISTANCIA"), 1).alias("VL_MAX_DIST"),
                f.round(f.min("DISTANCIA"), 1).alias("VL_MIN_DIST"),
                f.round(f.avg("DISTANCIA"), 1).alias("VL_AVG_DIST"),
                f.count(
                    f.when(f.lower(f.col("PROPOSITO")) == "reunião", True)
                ).alias("QT_CORR_REUNI"),
                f.count(
                    f.when(f.lower(f.col("PROPOSITO")) != "reunião", True)
                ).alias("QT_CORR_NAO_REUNI")
            ]

            df_gold = df_silver.groupBy("DT_REFE").agg(*agg_rules)
            self.logger.info("Agregação de dados concluída com sucesso.")
        except Exception as e:
            self.logger.error(f"Erro ao agregar os dados: {e}")
            raise e
        return df_gold