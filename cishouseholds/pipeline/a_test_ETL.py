from cishouseholds.pipeline.declare import add_ETL
from cishouseholds.pyspark_utils import get_or_create_spark_session


@add_ETL("a_test_ETL")
def a_test_ETL(path: str):
    spark_session = get_or_create_spark_session()
    a_test_ETL.has_been_called = True

    return spark_session.createDataFrame([], "col string")


a_test_ETL.has_been_called = False
