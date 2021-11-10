from chispa import assert_df_equality

from cishouseholds.derive import assign_latest_date


def test_assign_latest_date(spark_session):
    expected_df = spark_session.createDataFrame(
        data=[
            ("2021-07-20", "2019-10-11", "2007-04-09", "2021-07-20"),
            ("2011-08-20", "2016-10-21", "2045-01-11", "2045-01-11"),
            ("2011-01-02", "2011-01-03", "2011-01-04", "2011-01-04"),
        ],
        schema="date1 string, date2 string, date3 string, latest string",
    )
    output_df = assign_latest_date(expected_df.drop("latest"), "latest", ["date1", "date2", "date3"])
    assert_df_equality(output_df, expected_df, ignore_nullable=True)
