import pandas as pd
import pytest
from mimesis.schema import Schema

from cishouseholds.pipeline.input_file_stages import blood_delta_parameters
from cishouseholds.pipeline.input_file_stages import generate_input_processing_function
from dummy_data_generation.schemas import get_blood_data_description


@pytest.fixture
def blood_delta_ETL_output(mimesis_field, pandas_df_to_temporary_csv):
    """
    Generate lab bloods file.
    """
    schema = Schema(schema=get_blood_data_description(mimesis_field, "N"))
    pandas_df = pd.DataFrame(schema.create(iterations=5))
    csv_file_path = pandas_df_to_temporary_csv(pandas_df, filename="N.csv")
    processing_function = generate_input_processing_function(**blood_delta_parameters, include_hadoop_read_write=False)
    processed_df = processing_function(resource_path=csv_file_path)

    return processed_df


@pytest.mark.skip(reason="Lab data and merge currently out of scope")
@pytest.mark.regression
@pytest.mark.integration
def test_blood_delta_ETL_df(regression_test_df, blood_delta_ETL_output):
    regression_test_df(
        blood_delta_ETL_output.drop("blood_test_source_file"), "blood_sample_barcode", "processed_blood"
    )  # remove source file column, as it varies for our temp dummy data


@pytest.mark.skip(reason="Lab data and merge currently out of scope")
@pytest.mark.regression
@pytest.mark.integration
def test_blood_delta_ETL_schema(regression_test_df_schema, blood_delta_ETL_output):
    regression_test_df_schema(blood_delta_ETL_output, "processed_blood")
