from chispa import assert_df_equality

from cishouseholds.edit import update_from_csv_lookup


def test_update_from_csv_lookup(spark_session, pandas_df_to_temporary_csv):
    map_df = spark_session.createDataFrame(
        data=[
            # fmt: off
            (1, "B", "original",        "converted"),
            (2, "A", "original",        "converted"),
            (2, "B", "original",        "converted"),
            (3, "B", "original",        "converted"),
            (4, "B", "original",        "converted"),
            (5, "A", "something else",  "converted"),
            (6, "A", "converted A from null","converted A from null"),
            (6, "B", None,              "converted B from null"),

            (7, "B", 'haven\"t',         "havent"),
            (8, "B", "haven't",         "havent"),
            (9, "B", "have, no",        "have not"),
            # fmt: on
        ],
        schema="""id integer, target_column_name string, old_value string, new_value string""",
    )
    csv_filepath = pandas_df_to_temporary_csv(map_df.toPandas(), sep="|").as_posix()
    input_df = spark_session.createDataFrame(
        data=[
            (1, "original", "original"),
            (2, "original", "original"),
            (3, "original", "original"),
            (4, "original", "original"),
            (5, "original", "original"),
            (7, 'haven"t', 'haven"t'),
            (8, "haven't", "haven't"),
            (9, "have, no", "have, no"),
        ],
        schema="""id integer, A string, B string""",
    )
    expected_df = spark_session.createDataFrame(
        data=[
            # fmt: off
            (1, "original",                 "converted"),
            (2, "converted",                "converted"),
            (3, "original",                 "converted"),
            (4, "original",                 "converted"),
            (5, "original",                 "original"),
            (7, 'haven"t',                  "havent"),
            (8, "haven't",                  "havent"),
            (9, "have, no",                 "have not"),
            # fmt: on
        ],
        schema="""id integer, A string, B string""",
    )
    df = input_df
    map = csv_filepath

    output_df = update_from_csv_lookup(df, map, "id")
    assert_df_equality(expected_df, output_df, ignore_nullable=True, ignore_column_order=True)
