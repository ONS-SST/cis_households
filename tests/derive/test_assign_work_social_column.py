from chispa import assert_df_equality

from cishouseholds.derive import assign_work_social_column


def test_assign_work_social_colum(spark_session):
    expected_df = spark_session.createDataFrame(
        data=[
            ("No", "Self-employed", "No", "Yes"),
            ("Yes, care/residential home, resident-facing", "Furloughed (temporarily not working)", "Yes", "Yes"),
            ("Yes, other social care, non-resident-facing", "Furloughed (temporarily not working)", None, None),
            ("Yes, other social care, resident-facing", "Furloughed (temporarily not working)", "No", "Yes"),
        ],
        schema="work_socialcare string , work_sector string, work_care_nursing_home string,	work_direct_contact string",
    )
    output_df = assign_work_social_column(
        expected_df.drop("work_socialcare"),
        "work_socialcare",
        "work_sector",
        "work_care_nursing_home",
        "work_direct_contact",
    )
    assert_df_equality(output_df, expected_df, ignore_column_order=True)