import pytest
from pyspark.sql import SparkSession

@pytest.fixture
def cvr_penheder_latest_raw_df(spark: SparkSession):
    return spark.read.table(('cvr_penheder_latest_raw'))

def test_cvr_penheder_latest_raw_structure(cvr_penheder_latest_raw_df):
    expected_columns = [
        'sidstOpdateret', 'cvrNummer', 'pNummer', 'gyldigFra', 'gyldigTil', 'pen_sidstOpdateret'
    ]

    # Check if all expected columns are present
    assert set(cvr_penheder_latest_raw_df.columns) == set(expected_columns)

    # Check data types
    schema = dict(cvr_penheder_latest_raw_df.dtypes)
    schema['cvrNummer'] == 'bigint'
    schema['pNummer'] == 'bigint'
    schema['sidstOpdateret'] == 'string'
    schema['gyldigFra'] == 'string'
    schema['gyldigTil'] == 'string'
    schema['pen_sidstOpdateret'] = 'string'

def test_cvr_penheder_latest_raw_unique_pNummer(cvr_penheder_latest_raw_df):
    duplicates = cvr_penheder_latest_raw_df.groupBy('pNummer').count().filter('count > 1')
    assert duplicates.count() == 0, "Found duplicated pNummer values"