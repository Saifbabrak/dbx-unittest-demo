import pytest
from pyspark.sql import SparkSession

@pytest.fixture
def cvr_latest_metadata_df(spark: SparkSession):
    return spark.read.table('cvr_latest_virk_metadata_raw')


def test_cvr_virk_latest_metadata_raw_row_count(cvr_latest_metadata_df):
    assert cvr_latest_metadata_df.count() == 3000

def test_cvr_virk_latest_metadata_raw_cvr_duplicates(cvr_latest_metadata_df):
    assert cvr_latest_metadata_df.select('cvrNUmmer').distinct().count() == 3000

def test_cvr_virk_latest_metadata_raw_structure(cvr_latest_metadata_df):
    expected_columns = [
        'sidstOpdateret', 'cvrNummer', 'navn', 'kommuneNavn', 'kommuneKode',
        'kommune_gyldigFra', 'kommune_gyldigTil', 'branchekode', 'branchetekst',
        'hovedbranche_gyldigFra', 'hovedbranche_gyldigTil', 'vejnavn', 'husnummerFra',
        'husnummerTil', 'bogstavFra', 'bogstavTil', 'etage', 'sidedoer', 'conavn',
        'postnummer', 'postdistrikt', 'landekode', 'vejkode', 
        'beliggenhedsadresse_gyldigFra', 'beliggenhedsadresse_gyldigTil',
        'virksomhedsformkode', 'langBeskrivelse', 'ansvarligDataleverandoer',
        'nyestevirksomhedsform_gyldigFra', 'nyestevirksomhedsform_gyldigTil'
    ]

    # Check if all expected columns are present
    assert set(cvr_latest_metadata_df.columns) == set(expected_columns)

    # Check data types
    schema = dict(cvr_latest_metadata_df.dtypes)
    assert schema['sidstOpdateret'] == 'string'
    assert schema['cvrNummer'] == 'bigint'
