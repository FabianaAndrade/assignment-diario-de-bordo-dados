import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pipeline.data_reading import DataReading
from utils.Logger import Logger
from pipeline.data_transformation import DataTransformation
from pipeline.data_aggregation import DataAggregation
from pipeline.writer import Writter

def main(spark, source_folder, target_folder, csv_sep, column_dat_ref, partition_column):
    logger = Logger()
    reader = DataReading(spark=spark, logger=logger)
    transformer = DataTransformation(spark=spark, logger=logger)
    writter = Writter(logger=logger)
    aggregator = DataAggregation(logger=logger)
    
    logger.info("Iniciando o pipeline de processamento de dados de corridas de aplicativos de transporte.")

    csv_df = reader.read_csv_file(source_folder=source_folder, sep=csv_sep)
    
    logger.info(f"Schema do arquivo CSV origem:")
    csv_df.printSchema()

    logger.info(f"Criando dataframe para camada raw... ")
    df_raw = transformer.date_ref_column(df = csv_df, column_ref=column_dat_ref)
    logger.info(f"dataframe criado para camada raw com sucesso. Schema: ")
    df_raw.printSchema()

    logger.info(f"Persistindo dataframe como parquet na camada raw...")

    writter.write_parquet_file(df = df_raw, target_folder= target_folder, layer='raw', partition_column=partition_column)
    

    logger.info(f"Recuperando dataframe persistido na camada raw...")
    df_raw = reader.read_parquet_file(layer='raw', source_folder=target_folder)
    
    
    logger.info("Iniciando Transformações dos dados da camada raw para a camada silver...")
    df_silver = transformer.change_data_type(df=df_raw, column_name='DATA_INICIO', new_data_type='timestamp')
    df_silver = transformer.change_data_type(df=df_silver, column_name='DATA_FIM', new_data_type='timestamp')
    df_silver = transformer.replace_missing_values(df=df_silver, 
                                                            columns_names=['PROPOSITO', 
                                                                           'LOCAL_INICIO',
                                                                           'LOCAL_FIM'])

    logger.info(f"Dataframe transformado para camada silver com sucesso. Schema: ")
    df_silver.printSchema()

    logger.info(f"Persistindo dataframe como parquet na camada silver...")
    writter.write_parquet_file(df=df_silver, target_folder= target_folder, layer='silver', partition_column=partition_column)

    logger.info(f"Recuperando dataframe persistido na camada silver...")
    df_silver = reader.read_parquet_file(layer='silver', source_folder=target_folder)

    logger.info(f"Dataframe recuperado da camada silver com sucesso. Schema: ")
    df_silver.printSchema()

    logger.info("Iniciando agregação de dados para criar a base gold...")
    df_gold = aggregator.agg_corridas_dia(df_silver=df_silver)
    logger.info("Agregação de dados concluída com sucesso. Dataframe gold criado. Schema:")
    df_gold.printSchema()
    logger.info("Amostra dados gold: ")
    df_gold.show(3)

    logger.info(f"Persistindo dataframe como parquet na camada gold...")
    writter.write_parquet_file(df=df_gold, layer='gold', target_folder=target_folder, partition_column=partition_column)

    logger.info(f"Pipeline de processamento de dados de corridas de aplicativos de transporte concluído com sucesso.")

if __name__ == "__main__":
    load_dotenv()
    
    spark = SparkSession.builder \
        .appName("pipe_diario_de_bordo") \
        .getOrCreate()

    main(
        spark=spark,
        source_folder=os.getenv("SOURCE_DATA_FOLDER", "data/inputs"),
        target_folder=os.getenv("TARGET_DATA_FOLDER", "data/outputs"),
        csv_sep=os.getenv("CSV_SEP", ";"),
        column_dat_ref=os.getenv("COLUMN_DAT_REF", "DATA_INICIO"),
        partition_column=os.getenv("PART_COLUMN_NAME", "DT_REFE")
    )