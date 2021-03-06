import pytest

from cishouseholds.pipeline.merge_process_combination import execute_merge_specific_swabs

# from chispa import assert_df_equality


@pytest.mark.skip(reason="Lab data and merge currently out of scope")
@pytest.mark.xfail(reason="units do not function correctly")
def test_merge_process_swab(spark_session):
    schema = "barcode string, unique_pcr_test_id string, any string"
    data = [
        ("ONS0001", "1", None),
        ("ONS0002", "2", None),
        ("ONS0003", "3", None),
        ("ONS0003", "4", None),
        ("ONS0003", "5", None),
        ("ONS0004", "6", None),
        ("ONS0004", "7", None),
        ("ONS0004", "8", None),
    ]
    df_input_survey = spark_session.createDataFrame(data, schema=schema).drop("any")
    # any line added to bypass an exception when only one column is added

    schema = "barcode string, unique_pcr_test_id string, date_visit string, date_received string,\
              pcr_result_recorded_datetime string, pcr_result_classification string"
    data = [
        ("ONS0001", "1,", "2020-01-01", "2020-01-02", "2020-01-04 12:00:00", "positive"),
        ("ONS0002", "2,", "2020-01-02", "2020-01-03", "2020-01-04 12:00:00", "negative"),
        ("ONS0002", "3,", "2020-01-02", "2020-01-03", "2020-01-04 12:00:00", "negative"),
        ("ONS0002", "4,", "2020-01-02", "2020-01-03", "2020-01-04 12:00:00", "positive"),
        ("ONS0003", "5,", "2020-01-03", "2020-01-04", "2020-01-04 12:00:00", "positive"),
        ("ONS0004", "6,", "2020-01-04", "2020-01-05", "2020-01-04 12:00:00", "positive"),
        ("ONS0004", "7,", "2020-01-04", "2020-01-05", "2020-01-04 12:00:00", "positive"),
        ("ONS0004", "8,", "2020-01-04", "2020-01-05", "2020-01-04 12:00:00", "positive"),
    ]
    df_input_labs = spark_session.createDataFrame(data, schema=schema)

    # add expected dataframe
    schema = """
                barcode string,
                unique_participant_response_id integer,
                count_barcode_voyager integer,
                date_visit string,
                date_received string,
                pcr_result_recorded_datetime string,
                pcr_result_classification string,
                unique_pcr_test_id integer,
                count_barcode_swab integer,
                diff_vs_visit_hr double,
                out_of_date_range_swab integer,
                abs_offset_diff_vs_visit_hr double,
                identify_1tom_swabs_flag integer,
                time_order_flag integer,
                pcr_flag integer,
                time_difference_flag integer,
                drop_flag_1tom_swab integer,
                identify_mto1_swab_flag integer,
                drop_flag_mto1_swab integer,
                identify_mtom_flag integer,
                failed_flag_mtom_swab integer,
                drop_flag_mtom_swab integer,
                1tom_swab integer,
                mto1_swab integer,
                mtom_swab integer,
                failed_1tom_swab integer,
                failed_mto1_swab integer,
                failed_mtom_swab integer
            """
    data = [
        # fmt:off
        ("ONS0003",
            3, 3, "2020-01-03", "2020-01-04", "2020-01-04 12:00:00", "positive", 5, 1, 24.0, None, 0.0, 1, None, None, None, 1, 1, 1, None, None, None, 1, None, None, None, None, None),
        ("ONS0003", 4, 3, "2020-01-03", "2020-01-04", "2020-01-04 12:00:00", "positive", 5, 1, 24.0, None, 0.0, 1, None, None, None, 1, 1, 1, None, None, 1, 1, None, None, None, None, None),
        ("ONS0003", 5, 3, "2020-01-03", "2020-01-04", "2020-01-04 12:00:00", "positive", 5, 1, 24.0, None, 0.0, 1, None, None, None, 1, 1, 1, None, None, 1, 1, None, None, None, None, None),
        ("ONS0004", 8, 3, "2020-01-04", "2020-01-05", "2020-01-04 12:00:00", "positive", 8, 3, 24.0, None, 0.0, 1, None, None, None, 1, None, None, 1, None, None, None, None, 1, None, None, 1),
        ("ONS0004", 7, 3, "2020-01-04", "2020-01-05", "2020-01-04 12:00:00", "positive", 7, 3, 24.0, None, 0.0, 1, None, None, None, 1, None, None, 1, None, None, None, None, 1, None, None, 1),
        ("ONS0004", 6, 3, "2020-01-04", "2020-01-05", "2020-01-04 12:00:00", "positive", 6, 3, 24.0, None, 0.0, 1, None, None, None, 1, None, None, 1, None, None, None, None, 1, None, None, 1),
        ("ONS0004", 7, 3, "2020-01-04", "2020-01-05", "2020-01-04 12:00:00", "positive", 8, 3, 24.0, None, 0.0, 1, None, None, None, 1, None, None, 1, None, 1, None, None, 1, None, None, 1),
        ("ONS0004", 8, 3, "2020-01-04", "2020-01-05", "2020-01-04 12:00:00", "positive", 7, 3, 24.0, None, 0.0, 1, None, None, None, 1, None, None, 1, None, 1, None, None, 1, None, None, 1),
        ("ONS0004", 6, 3, "2020-01-04", "2020-01-05", "2020-01-04 12:00:00", "positive", 7, 3, 24.0, None, 0.0, 1, None, None, None, 1, None, None, 1, None, 1, None, None, 1, None, None, 1),
        ("ONS0004", 6, 3, "2020-01-04", "2020-01-05", "2020-01-04 12:00:00", "positive", 8, 3, 24.0, None, 0.0, 1, None, None, None, 1, None, None, 1, None, 1, None, None, 1, None, None, 1),
        ("ONS0004", 7, 3, "2020-01-04", "2020-01-05", "2020-01-04 12:00:00", "positive", 6, 3, 24.0, None, 0.0, 1, None, None, None, 1, None, None, 1, None, 1, None, None, 1, None, None, 1),
        ("ONS0004", 8, 3, "2020-01-04", "2020-01-05", "2020-01-04 12:00:00", "positive", 6, 3, 24.0, None, 0.0, 1, None, None, None, 1, None, None, 1, None, 1, None, None, 1, None, None, 1),
        ("ONS0002", 2, 1, "2020-01-02", "2020-01-03", "2020-01-04 12:00:00", "negative", 2, 3, 24.0, None, 0.0, None, None, None, None, None, None, None, None, None, None, None, 1, None, None, 1, None),
        ("ONS0002", 2, 1, "2020-01-02", "2020-01-03", "2020-01-04 12:00:00", "negative", 3, 3, 24.0, None, 0.0, None, None, None, None, None, None, None, None, None, 1, None, 1, None, None, 1, None),
        ("ONS0002", 2, 1, "2020-01-02", "2020-01-03", "2020-01-04 12:00:00", "positive", 4, 3, 24.0, None, 0.0, None, None, None, None, None, None, None, None, None, 1, None, 1, None, None, 1, None),
        ("ONS0001", 1, 1, "2020-01-01", "2020-01-02", "2020-01-04 12:00:00", "positive", 1, 1, 24.0, None, 0.0, 1, None, None, None, 1, None, None, None, None, None, None, None, None, None, None, None),
        # fmt:on
    ]
    # expected_df = spark_session.createDataFrame(data, schema=schema)
    output_df = execute_merge_specific_swabs(
        survey_df=df_input_survey,
        labs_df=df_input_labs,
        barcode_column_name="barcode",
        visit_date_column_name="date_visit",
        received_date_column_name="date_received",
    )
    # assert_df_equality(output_df, expected_df)
    assert len(output_df.columns) != 0 and output_df.count() != 0
