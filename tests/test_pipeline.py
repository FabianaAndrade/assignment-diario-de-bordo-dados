
'''
Modulo com testes para testar o pipeline.
Avalia os componentes de leitura, agregação e escrita de dados.
'''
import pytest
from pydantic import ValidationError
from unittest.mock import MagicMock, patch
from pipeline.data_reading import DataReading
from pipeline.data_aggregation import DataAggregation
from pipeline.writer import Writter
from pyspark.sql import Row


@pytest.fixture(scope="session")
def spark():
    from pyspark.sql import SparkSession
    spark = SparkSession.builder \
        .appName("pytest_reader") \
        .master("local[*]") \
        .getOrCreate()
    yield spark
    spark.stop()

@pytest.fixture
def mock_logger():
    return MagicMock()

@pytest.fixture
def reader(spark, mock_logger):
    return DataReading(spark=spark, logger=mock_logger)

@pytest.fixture
def tmp_csv_file(tmp_path):
    data_path = tmp_path / "test_data"
    data_path.mkdir()
    csv_file = data_path / "dados.csv"
    csv_file.write_text("local_inicio;local_fim\nvilaolimpia;pinheiros\nitaquera;tatuape")
    return str(data_path)

@pytest.fixture
def tmp_parquet_folder(spark, tmp_path):
    df = spark.createDataFrame([("vilaolimpia", "pinheiros"), ("itaquera", "tatuape")], ["local_inicio", "local_fim"])
    parquet_path = tmp_path / "silver"
    df.write.parquet(str(parquet_path))
    return str(tmp_path)


# test leitura de dados

class TestDataReading:

    def test_read_csv_file(self, reader, tmp_csv_file):
        df = reader.read_csv_file(tmp_csv_file, sep=';')
        assert df.count() == 2
        assert "local_inicio" in df.columns
        assert "local_fim" in df.columns
        reader.logger.info.assert_any_call(f"Iniciando leitura do arquivo CSV da pasta: {tmp_csv_file}")

    def test_read_parquet_file(self, reader, tmp_parquet_folder):
        df = reader.read_parquet_file(layer="silver", source_folder=tmp_parquet_folder)
        assert df.count() == 2
        assert "local_inicio" in df.columns
        assert "local_fim" in df.columns
        reader.logger.info.assert_any_call("Iniciando leitura dos arquivos parquet da camada: silver")

# test aggregation

@pytest.fixture
def aggregator(mock_logger):
    return DataAggregation(logger=mock_logger)

@pytest.fixture
def silver_df(spark):
    dados = [
        Row(DT_REFE="2016-08-01", CATEGORIA="Negocio", DISTANCIA=10.5, PROPOSITO="Reunião"),
        Row(DT_REFE="2016-08-01", CATEGORIA="Pessoal", DISTANCIA=8.0, PROPOSITO="Nao Reunião"),
        Row(DT_REFE="2016-08-01", CATEGORIA="Negocio", DISTANCIA=12.3, PROPOSITO="Reunião"),
        Row(DT_REFE="2016-08-02", CATEGORIA="Negocio", DISTANCIA=5.7, PROPOSITO="Reunião"),
        Row(DT_REFE="2016-08-02", CATEGORIA="Pessoal", DISTANCIA=7.2, PROPOSITO="Nao Reunião"),
    ]
    return spark.createDataFrame(dados)

class TestDataAggregation:
    def test_agg_corridas_dia(self, aggregator, silver_df):
        df_gold = aggregator.agg_corridas_dia(silver_df)

        assert df_gold.count() == 2

        expected_columns = [
            "DT_REFE", "QT_CORR", "QT_CORR_NEG", "QT_CORR_PESS",
            "VL_MAX_DIST", "VL_MIN_DIST", "VL_AVG_DIST",
            "QT_CORR_REUNI", "QT_CORR_NAO_REUNI"
        ]
        for col in expected_columns:
            assert col in df_gold.columns

        resultado = df_gold.filter("DT_REFE = '2016-08-01'").collect()[0]
        assert resultado.QT_CORR == 3
        assert resultado.QT_CORR_NEG == 2
        assert resultado.QT_CORR_PESS == 1
        assert resultado.QT_CORR_REUNI == 2
        assert resultado.QT_CORR_NAO_REUNI == 1


# teste writter
@pytest.fixture
def writer(mock_logger):
    return Writter(logger=mock_logger)

@pytest.fixture
def sample_df_silver(spark):
    dados = [
        Row(Categoria="Pessoal", Distancia=30, data="2026-10-01"),
        Row(Categoria="Negocio", Distancia=25, data="2026-11-02"),
        Row(Categoria="Pessoal", Distancia=40, data="2026-12-03"),
    ]
    return spark.createDataFrame(dados)

def test_write_parquet_file(tmp_path, writer, sample_df_silver):
    target_folder = tmp_path
    layer = "gold"
    partition_column = "data"

    writer.write_parquet_file(
        df=sample_df_silver,
        target_folder=str(target_folder),
        layer=layer,
        partition_column=partition_column
    )

    output_path = target_folder / layer
    assert output_path.exists()
    assert any(output_path.glob("*.parquet")) or any(output_path.glob("*/part-*"))

    writer.logger.info.assert_any_call(f"Escrevendo o DataFrame na camada {layer} no diretorio: {output_path}.")
    writer.logger.info.assert_any_call("DataFrame persistido com sucesso!")
