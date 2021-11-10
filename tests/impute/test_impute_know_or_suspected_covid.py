from chispa import assert_df_equality

from cishouseholds.impute import impute_known_or_suspected_covid_latest_date


def test_impute_known_or_suspected_covid_latest_date(spark_session):
    input_df = spark_session.createDataFrame(
        data=[
            ("F", "2000-01-01", 5, "1998-05-14"),
            ("D", "2000-01-01", 3, None),
            ("B", "2000-01-01", 1, "2011-01-04"),
            ("E", "2000-01-01", 4, "1999-07-20"),
            ("A", "2000-01-01", 0, "2045-01-11"),
            ("C", "2000-01-01", 2, "2011-01-04"),
        ],
        schema="id string, visit_date string, contact integer, contact_date string",
    )

    expected_df = spark_session.createDataFrame(
        data=[
            ("A", "2000-01-01", 0, "2045-01-11", None),
            ("B", "2000-01-01", 1, "2011-01-04", 1),  # flag due to covid contact == 1
            ("C", "2000-01-01", 2, "2011-01-04", 1),  # flag due to previous row covid contact == 1
            ("D", "2000-01-01", 3, None, 1),  # flag due to null date
            ("E", "2000-01-01", 4, "1999-07-20", None),
            (
                "F",
                "2000-01-01",
                5,
                "1998-05-14",
                1,
            ),  # flag due to contact date less than previous row contact date andvisit date greater than previous row contact date
        ],
        schema="id string, visit_date string, contact integer, contact_date string, flag integer",
    )

    output_df = impute_known_or_suspected_covid_latest_date(
        input_df, "flag", ["visit_date", "id"], "contact", "contact_date", "visit_date"
    )
    assert_df_equality(output_df, expected_df, ignore_column_order=True)
