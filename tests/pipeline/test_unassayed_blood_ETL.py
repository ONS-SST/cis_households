import pandas as pd
import pytest
from mimesis.schema import Schema

from cishouseholds.pipeline.input_file_stages import generate_input_processing_function
from cishouseholds.pipeline.input_file_stages import unassayed_blood_delta_parameters
from dummy_data_generation.schemas import get_unassayed_blood_data_description


@pytest.fixture
def unassayed_blood_ETL_output(mimesis_field, pandas_df_to_temporary_csv):
    """
    Generate unassayed bloods file.
    """
    schema = Schema(schema=get_unassayed_blood_data_description(mimesis_field))
    pandas_df = pd.DataFrame(schema.create(iterations=5))
    csv_file_path = pandas_df_to_temporary_csv(pandas_df)
    processing_function = generate_input_processing_function(
        **unassayed_blood_delta_parameters, include_hadoop_read_write=False
    )
    processed_df = processing_function(resource_path=csv_file_path.as_posix())
    return processed_df


@pytest.mark.skip(reason="Lab data and merge currently out of scope")
@pytest.mark.regression
@pytest.mark.integration
def test_unassayed_blood_ETL_df(regression_test_df, unassayed_blood_ETL_output):
    regression_test_df(
        unassayed_blood_ETL_output.drop("unassayed_blood_source_file"), "blood_sample_barcode", "unassayed_blood"
    )  # remove source file column, as it varies for our temp dummy data


@pytest.mark.skip(reason="Lab data and merge currently out of scope")
@pytest.mark.regression
@pytest.mark.integration
def test_unassayed_blood_ETL_schema(regression_test_df_schema, unassayed_blood_ETL_output):
    regression_test_df_schema(unassayed_blood_ETL_output, "unassayed_blood")
