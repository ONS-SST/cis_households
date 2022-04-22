from cishouseholds.pipeline.blood_delta_ETL import add_historical_fields
from cishouseholds.pipeline.blood_delta_ETL import transform_blood_delta
from cishouseholds.pipeline.cast_columns_from_string_map import survey_response_cast_to_double
from cishouseholds.pipeline.historical_blood_ETL import add_fields
from cishouseholds.pipeline.input_variable_names import blood_variable_name_map
from cishouseholds.pipeline.input_variable_names import historical_blood_variable_name_map
from cishouseholds.pipeline.input_variable_names import survey_responses_v0_variable_name_map
from cishouseholds.pipeline.input_variable_names import survey_responses_v1_variable_name_map
from cishouseholds.pipeline.input_variable_names import survey_responses_v2_variable_name_map
from cishouseholds.pipeline.input_variable_names import swab_variable_name_map
from cishouseholds.pipeline.input_variable_names import unassayed_bloods_variable_name_map
from cishouseholds.pipeline.pipeline_stages import generate_input_processing_function
from cishouseholds.pipeline.survey_responses_version_0_ETL import transform_survey_responses_version_0_delta
from cishouseholds.pipeline.survey_responses_version_1_ETL import clean_survey_responses_version_1
from cishouseholds.pipeline.survey_responses_version_1_ETL import transform_survey_responses_version_1_delta
from cishouseholds.pipeline.survey_responses_version_2_ETL import clean_survey_responses_version_2
from cishouseholds.pipeline.survey_responses_version_2_ETL import derive_additional_v1_2_columns
from cishouseholds.pipeline.survey_responses_version_2_ETL import transform_survey_responses_generic
from cishouseholds.pipeline.survey_responses_version_2_ETL import transform_survey_responses_version_2_delta
from cishouseholds.pipeline.swab_delta_ETL import transform_swab_delta
from cishouseholds.pipeline.swab_delta_ETL_testKit import transform_swab_delta_testKit
from cishouseholds.pipeline.timestamp_map import blood_datetime_map
from cishouseholds.pipeline.timestamp_map import survey_responses_v0_datetime_map
from cishouseholds.pipeline.timestamp_map import survey_responses_v1_datetime_map
from cishouseholds.pipeline.timestamp_map import survey_responses_v2_datetime_map
from cishouseholds.pipeline.timestamp_map import swab_datetime_map
from cishouseholds.pipeline.unassayed_blood_ETL import transform_unassayed_blood
from cishouseholds.pipeline.validation_schema import blood_validation_schema
from cishouseholds.pipeline.validation_schema import historical_blood_validation_schema
from cishouseholds.pipeline.validation_schema import survey_responses_v0_validation_schema
from cishouseholds.pipeline.validation_schema import survey_responses_v1_validation_schema
from cishouseholds.pipeline.validation_schema import survey_responses_v2_validation_schema
from cishouseholds.pipeline.validation_schema import swab_validation_schema
from cishouseholds.pipeline.validation_schema import swab_validation_schema_testKit
from cishouseholds.pipeline.validation_schema import unassayed_blood_validation_schema

blood_delta_parameters = {
    "stage_name": "blood_delta_ETL",
    "dataset_name": "antibody_test_results",
    "id_column": "blood_sample_barcode",
    "validation_schema": blood_validation_schema,
    "column_name_map": blood_variable_name_map,
    "datetime_column_map": blood_datetime_map,
    "transformation_functions": [transform_blood_delta, add_historical_fields],
    "source_file_column": "blood_test_source_file",
    "write_mode": "append",
}

swab_delta_parameters = {
    "stage_name": "swab_delta_ETL",
    "dataset_name": "pcr_test_results",
    "id_column": "swab_sample_barcode",
    "validation_schema": swab_validation_schema,
    "column_name_map": swab_variable_name_map,
    "datetime_column_map": swab_datetime_map,
    "transformation_functions": [transform_swab_delta],
    "source_file_column": "swab_test_source_file",
    "write_mode": "append",
}

swab_delta_parameters_testKit = {
    "stage_name": "swab_testKit_delta_ETL",
    "dataset_name": "pcr_test_results",
    "id_column": "swab_sample_barcode",
    "validation_schema": swab_validation_schema_testKit,
    "column_name_map": swab_variable_name_map,
    "datetime_column_map": swab_datetime_map,
    "transformation_functions": [transform_swab_delta_testKit, transform_swab_delta],
    "source_file_column": "swab_test_source_file",
    "write_mode": "append",
}

survey_responses_v2_parameters = {
    "stage_name": "survey_responses_version_2_ETL",
    "dataset_name": "survey_responses_v2",
    "id_column": "visit_id",
    "validation_schema": survey_responses_v2_validation_schema,
    "column_name_map": survey_responses_v2_variable_name_map,
    "datetime_column_map": survey_responses_v2_datetime_map,
    "transformation_functions": [
        transform_survey_responses_generic,
        clean_survey_responses_version_2,
        derive_additional_v1_2_columns,
        transform_survey_responses_version_2_delta,
    ],
    "sep": "|",
    "cast_to_double_list": survey_response_cast_to_double,
    "source_file_column": "survey_response_source_file",
}

survey_responses_v1_parameters = {
    "stage_name": "survey_responses_version_1_ETL",
    "dataset_name": "survey_responses_v1",
    "id_column": "visit_id",
    "validation_schema": survey_responses_v1_validation_schema,
    "column_name_map": survey_responses_v1_variable_name_map,
    "datetime_column_map": survey_responses_v1_datetime_map,
    "transformation_functions": [
        transform_survey_responses_generic,
        clean_survey_responses_version_1,
        derive_additional_v1_2_columns,
        transform_survey_responses_version_1_delta,
    ],
    "sep": "|",
    "cast_to_double_list": survey_response_cast_to_double,
    "source_file_column": "survey_response_source_file",
}

survey_responses_v0_parameters = {
    "stage_name": "survey_responses_version_0_ETL",
    "dataset_name": "survey_responses_v0",
    "id_column": "visit_id",
    "validation_schema": survey_responses_v0_validation_schema,
    "column_name_map": survey_responses_v0_variable_name_map,
    "datetime_column_map": survey_responses_v0_datetime_map,
    "transformation_functions": [
        transform_survey_responses_generic,
        transform_survey_responses_version_0_delta,
    ],
    "sep": "|",
    "cast_to_double_list": survey_response_cast_to_double,
    "source_file_column": "survey_response_source_file",
}

unassayed_blood_delta_parameters = {
    "stage_name": "unassayed_blood_ETL",
    "dataset_name": "unassayed_blood_delta",
    "id_column": "blood_sample_barcode",
    "validation_schema": unassayed_blood_validation_schema,
    "column_name_map": unassayed_bloods_variable_name_map,
    "datetime_column_map": blood_datetime_map,
    "transformation_functions": [transform_unassayed_blood],
    "source_file_column": "unassayed_blood_source_file",
    "write_mode": "append",
}

historical_blood_parameters = {
    "stage_name": "historical_blood_ETL",
    "dataset_name": "antibody_test_results",
    "id_column": "blood_sample_barcode",
    "validation_schema": historical_blood_validation_schema,
    "column_name_map": historical_blood_variable_name_map,
    "datetime_column_map": blood_datetime_map,
    "transformation_functions": [transform_blood_delta, add_fields],
    "source_file_column": "blood_test_source_file",
}

for parameters in [
    blood_delta_parameters,
    swab_delta_parameters,
    swab_delta_parameters_testKit,
    survey_responses_v2_parameters,
    survey_responses_v1_parameters,
    survey_responses_v0_parameters,
    unassayed_blood_delta_parameters,
    historical_blood_parameters,
]:
    generate_input_processing_function(**parameters)
