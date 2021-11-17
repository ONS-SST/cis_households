import subprocess
from datetime import datetime
from typing import List
from typing import Optional
from typing import Union

import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

from cishouseholds.pipeline.config import get_config
from cishouseholds.pipeline.load import add_error_file_log_entry
from cishouseholds.pipeline.load import check_table_exists
from cishouseholds.pyspark_utils import get_or_create_spark_session
from cishouseholds.validate import validate_csv_fields
from cishouseholds.validate import validate_csv_header


class InvalidFileError(Exception):
    pass


def read_csv_to_pyspark_df(
    spark_session: SparkSession,
    csv_file_path: Union[str, list],
    expected_raw_header_row: str,
    schema: StructType,
    sep: str = ",",
    **kwargs,
) -> DataFrame:
    """
    Validate and read a csv file into a PySpark DataFrame.

    Parameters
    ----------
    csv_file_path
        file to read to dataframe
    expected_raw_header_row
        expected first line of file
    schema
        schema to use for returned dataframe, including desired column names

    Takes keyword arguments from ``pyspark.sql.DataFrameReader.csv``,
    for example ``timestampFormat="yyyy-MM-dd HH:mm:ss 'UTC'"``.
    """
    spark_session = get_or_create_spark_session()
    if not isinstance(csv_file_path, list):
        csv_file_path = [csv_file_path]

    for csv_file in csv_file_path:
        text_file = spark_session.sparkContext.textFile(csv_file)
        csv_header = validate_csv_header(text_file, expected_raw_header_row)
        csv_fields = validate_csv_fields(text_file, delimiter=sep)

        if not csv_header:
            print(
                f"FILE ERROR: Header of {csv_file} ({text_file.first()}) "
                f"does not match expected header: {expected_raw_header_row}"
            )  # functional
            error = True

        if not csv_fields:
            print(
                f"FILE ERROR: Number of fields in {csv_file} does not match expected number of columns from header"
            )  # functional
            error = True
        if error:
            add_error_file_log_entry(get_config(), csv_file, datetime.now())

    return spark_session.read.csv(
        csv_file_path,
        header=True,
        schema=schema,
        ignoreLeadingWhiteSpace=True,
        ignoreTrailingWhiteSpace=True,
        sep=sep,
        **kwargs,
    )


def get_date_from_filename(filename: str, sep: Optional[str] = "_", format: Optional[str] = "%Y%m%d") -> str:
    """
    Get a date string from a filename of containing a string formatted date
    Parameters
    ----------
    filename
    sep
    format
    """
    try:
        file_date = filename.split(sep)[-1].split(".")[0]
        file_date = datetime.strptime(file_date, format)  # type: ignore
        return file_date
    except ValueError:
        return str(None)


def list_contents(
    path: str, recursive: Optional[bool] = False, date_from_filename: Optional[bool] = False
) -> DataFrame:
    """
    Read contents of a directory and return the path for each file and
    returns a dataframe of
    Parameters
    ----------
    path : String
    recursive
    """
    command = ["hadoop", "fs", "-ls"]
    if recursive:
        command.append("-R")
    ls = subprocess.Popen([*command, path], stdout=subprocess.PIPE)
    names = ["permission", "id", "owner", "group", "value", "upload_date", "upload_time", "file_path"]
    files = []
    for line in ls.stdout:  # type: ignore
        dic = {}
        f = line.decode("utf-8")
        if "Found" not in f:
            for i, component in enumerate(f.split()):
                dic[names[i]] = component
            dic["filename"] = dic["file_path"].split("/")[-1]
            if date_from_filename:
                file_date = get_date_from_filename(dic["filename"])
                if file_date is not None:
                    dic["upload_date"] = file_date
            files.append(dic)
    return pd.DataFrame(files)


def get_files_by_date(
    path: str,
    latest_only: bool = False,
    start_date: Optional[Union[str, datetime]] = None,
    end_date: Optional[Union[str, datetime]] = None,
) -> List:
    """
    Get a list of hdfs file paths for a given set of date critera and parent path on hdfs.
    Optionally select the latest only.

    Parameters
    ----------
    path
        hdfs file path to search
    selector
        options for selection type: latest, after(date), before(date)
    start_date
        date to select files after
    end_date
        date to select files before
    """
    file_df = list_contents(path, date_from_filename=True)
    file_df = file_df.sort_values(["upload_date", "upload_time"])

    if start_date is not None:
        if isinstance(start_date, str):
            start_date = datetime.strptime(start_date, "%Y-%m-%d")
        file_df = file_df[file_df["upload_date"].dt.date >= start_date]
    if end_date is not None:
        if isinstance(end_date, str):
            end_date = datetime.strptime(end_date, "%Y-%m-%d")
        file_df = file_df[file_df["upload_date"].dt.date <= end_date]

    file_list = file_df["file_path"].tolist()
    if latest_only:
        file_list = [file_list[-1]]
    return file_list


def get_files_not_processed(file_list: list, table_name: str):
    """
    Returns a list of files (to process) which haven't yet been processed
    Parameters
    ----------
    file_list: List of all files for processing
    table_name: Hive table which contains the list of files already processed
    ----------
    """
    spark_session = get_or_create_spark_session()
    storage_config = get_config()["storage"]
    if check_table_exists(table_name):
        df_processed = spark_session.sql(
            f"SELECT DISTINCT processed_filename FROM \
            {storage_config['database']}.{storage_config['table_prefix']}{table_name}"
        )
        processed_file_list = [row[0] for row in df_processed.collect()]

        unprocessed_file_list = [i for i in file_list if i not in processed_file_list]

        return unprocessed_file_list

    else:
        return file_list


def get_files_to_be_processed(
    resource_path, latest_only=False, start_date=None, end_date=None, include_processed=False
):
    """
    Get list of files matching the specified pattern and optionally filter
    to only those that have not been processed.
    """
    file_paths = get_files_by_date(resource_path, latest_only, start_date, end_date)
    if not include_processed:
        file_paths = get_files_not_processed(file_paths, "processed_filenames")
    return file_paths
