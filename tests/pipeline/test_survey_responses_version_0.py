import pandas as pd
import pytest
from mimesis.schema import Schema

from cishouseholds.pipeline.ETL_scripts import extract_validate_transform_input_data
from cishouseholds.pipeline.input_variable_names import survey_responses_v0_variable_name_map
from cishouseholds.pipeline.survey_responses_version_0_ETL import transform_survey_responses_version_0_delta
from cishouseholds.pipeline.survey_responses_version_2_ETL import transform_survey_responses_generic
from cishouseholds.pipeline.timestamp_map import survey_responses_datetime_map
from cishouseholds.pipeline.validation_schema import survey_responses_v0_validation_schema
from dummy_data_generation.schemas import get_voyager_0_data_description


@pytest.fixture
def responses_v0_survey_ETL_output(mimesis_field, pandas_df_to_temporary_csv):
    """
    Generate dummy survey responses v0 delta.
    """
    schema = Schema(schema=get_voyager_0_data_description(mimesis_field, ["ONS00000000"], ["ONS00000000"]))
    pandas_df = pd.DataFrame(schema.create(iterations=10))
    csv_file_path = pandas_df_to_temporary_csv(pandas_df, sep="|")
    processed_df = extract_validate_transform_input_data(
        csv_file_path.as_posix(),
        survey_responses_v0_variable_name_map,
        survey_responses_datetime_map,
        survey_responses_v0_validation_schema,
        [transform_survey_responses_generic, transform_survey_responses_version_0_delta],
        "|",
    )
    return processed_df


@pytest.mark.integration
def test_responses_version_0_delta_df(responses_v0_survey_ETL_output, regression_test_df):
    regression_test_df(
        responses_v0_survey_ETL_output.drop("survey_responses_v0_source_file"), "visit_id", "processed_responses_v0"
    )  # remove source file column, as it varies for our temp dummy data


@pytest.mark.integration
def test_responses_version_0_delta_schema(regression_test_df_schema, responses_v0_survey_ETL_output):
    regression_test_df_schema(responses_v0_survey_ETL_output, "processed_responses_v0")
