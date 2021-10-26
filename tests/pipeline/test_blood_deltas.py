import pandas as pd
import pytest
from mimesis.schema import Schema

from cishouseholds.pipeline.blood_delta_ETL import extract_validate_transform_blood_delta
from dummy_data_generation.schemas import get_blood_data_description


@pytest.fixture
def blood_delta_ETL_output(mimesis_field, pandas_df_to_temporary_csv):
    """
    Generate lab bloods file.
    """
    schema = Schema(schema=get_blood_data_description(mimesis_field, "N"))
    pandas_df = pd.DataFrame(schema.create(iterations=5))
    csv_file_path = pandas_df_to_temporary_csv(pandas_df)
    processed_df = extract_validate_transform_blood_delta(csv_file_path.as_posix())

    return processed_df


@pytest.mark.integration
def test_blood_delta_ETL_df(regression_test_df, blood_delta_ETL_output):
    regression_test_df(
        blood_delta_ETL_output.drop("blood_test_source_file"), "blood_sample_barcode", "processed_blood"
    )  # remove source file column, as it varies for our temp dummy data


@pytest.mark.integration
def test_blood_delta_ETL_schema(regression_test_df_schema, blood_delta_ETL_output):
    regression_test_df_schema(blood_delta_ETL_output, "processed_blood")
