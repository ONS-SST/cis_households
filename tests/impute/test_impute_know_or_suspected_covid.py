from chispa import assert_df_equality

from cishouseholds.impute import impute_known_or_suspected_covid_latest_date


def test_impute_known_or_suspected_covid_latest_date(spark_session):
    input_df = spark_session.createDataFrame(
        data=[
            ("A", "A", "A", "2000-01-01", 5, "1998-05-14", "2019-08-17"),
            ("C", "A", "A", "2000-01-01", 3, None, None),
            ("E", "A", "A", "2000-01-01", 1, "2011-01-04", "2019-05-10"),
            ("B", "A", "A", "2000-01-01", 4, "1999-07-20", None),
            ("F", "A", "A", "2000-01-01", 0, "2045-01-11", "2019-10-05"),
            ("D", "A", "A", "2000-01-01", 2, "2011-01-04", None),
        ],
        schema="id string, suspect_type string, known_type string, visit_date string, contact integer, contact_date string, impute string",
    )

    expected_df = spark_session.createDataFrame(
        data=[
            ("F", "A", "A", "2000-01-01", 0, "2045-01-11", None),
            ("E", "A", "A", "2000-01-01", 1, "2011-01-04", 1),  # flag due to covid contact == 1
            ("D", "A", "A", "2000-01-01", 2, "2011-01-04", 1),  # flag due to previous row covid contact == 1
            ("C", "A", "A", "2000-01-01", 3, None, 1),  # flag due to null date
            ("B", "A", "A", "2000-01-01", 4, "1999-07-20", None),
            (
                "A",
                "A",
                "A",
                "2000-01-01",
                5,
                "1998-05-14",
                1,
            ),
            # flag due to contact date less than previous row contact date andvisit date greater than previous row contact date
        ],
        schema="id string, suspect_type string, known_type string, visit_date string, contact integer, contact_date string, flag integer",
    )

    output_df = impute_known_or_suspected_covid_latest_date(
        input_df, "impute", ["suspect_type", "known_type", "visit_date", "id"], "contact", "contact_date", "visit_date"
    )
    # assert_df_equality(output_df, expected_df, ignore_column_order=True)
