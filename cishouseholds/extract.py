import csv
import re
import subprocess
from datetime import datetime
from io import StringIO
from typing import List
from typing import Optional
from typing import Union

import pandas as pd
from pyspark.sql import DataFrame

from cishouseholds.pipeline.load import extract_from_table
from cishouseholds.pyspark_utils import column_to_list
from cishouseholds.pyspark_utils import get_or_create_spark_session


class InvalidFileError(Exception):
    pass


def normalise_schema(file_path: str, reference_validation_schema: dict, regex_schema: dict):
    """
    Use a series of regex patterns mapped to correct column names to build an individual schema
    for a given csv input file that has varied headings across a group of similar files.
    """
    spark_session = get_or_create_spark_session()

    file = spark_session.sparkContext.textFile(file_path)
    actual_header = file.first()
    buffer = StringIO(actual_header)
    reader = csv.reader(buffer, delimiter=",")
    actual_header = next(reader)
    validation_schema = {}
    column_name_map = {}
    dont_drop_list = []
    if actual_header != list(reference_validation_schema.keys()):
        for actual_col in actual_header:
            validation_schema[actual_col] = {"type": "string"}
            for regex, normalised_column in regex_schema.items():
                if re.search(rf"{regex}", actual_col):
                    validation_schema[actual_col] = reference_validation_schema[normalised_column]
                    column_name_map[actual_col] = normalised_column
                    dont_drop_list.append(actual_col)
                    break
        return validation_schema, column_name_map, [col for col in actual_header if col not in dont_drop_list]
    return reference_validation_schema, {}, []


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
        attributes = f.split()

        attributes = [*attributes[:7], " ".join(attributes[7:])]
        if "Found" not in f:
            for i, component in enumerate(attributes):
                dic[names[i]] = component
            dic["filename"] = dic["file_path"].split("/")[-1]
        files.append(dic)
    df = pd.DataFrame(files)
    if date_from_filename:
        df["upload_date"] = df["filename"].str.extract((r"(\d{8})(?:_\d{4}|_\d{6})?(?=.csv|.txt)"), expand=False)
        df["upload_date"] = pd.to_datetime(df["upload_date"], errors="coerce", format="%Y%m%d")
    return df


def get_files_by_date(
    path: str,
    start_date: Optional[Union[str, datetime]] = None,
    end_date: Optional[Union[str, datetime]] = None,
) -> List:
    """
    Get a list of hdfs file paths for a given set of date critera and parent path on hdfs.

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
    file_df = file_df.dropna(subset=["upload_date"])
    file_df = file_df.sort_values(["upload_date", "upload_time"])

    print(file_df)

    if start_date is not None:
        if isinstance(start_date, str):
            start_date = datetime.strptime(start_date, "%Y-%m-%d")
        file_df = file_df[file_df["upload_date"].dt.date >= start_date]
    if end_date is not None:
        if isinstance(end_date, str):
            end_date = datetime.strptime(end_date, "%Y-%m-%d")
        file_df = file_df[file_df["upload_date"].dt.date <= end_date]

    file_list = file_df["file_path"].tolist()
    print("FILE LIST INNER: ",file_list)
    return file_list


def remove_list_items_in_table(item_list: list, table_name: str, item_column: str):
    """
    Returns a list with items removed that exist in a specified table column.
    Used for removed previously processed or errored files.

    Parameters
    ----------
    item_list
        items to remove from
    table_name
        name of HIVE table that contains the list of items to remove
    item_column
        name of column in table containing items to remove from list
    """
    table_item_column = extract_from_table(table_name).select(item_column).distinct()
    table_items = column_to_list(table_item_column, item_column)

    item_list = [i for i in item_list if i not in table_items]
    return item_list


def get_files_to_be_processed(
    resource_path,
    latest_only=False,
    start_date=None,
    end_date=None,
    include_processed=False,
    include_invalid=False,
):
    """
    Get list of files matching the specified pattern and optionally filter
    to only those that have not been processed or were previously invalid.
    """
    from cishouseholds.pipeline.load import check_table_exists

    file_paths = get_files_by_date(resource_path, start_date, end_date)

    if check_table_exists("error_file_log") and not include_invalid:
        file_paths = remove_list_items_in_table(file_paths, "error_file_log", "file_path")
    if latest_only and len(file_paths) > 0:
        file_paths = [file_paths[-1]]
    if check_table_exists("processed_filenames") and not include_processed:
        # After latest_only as we don't want to process earlier files if the latest has already been processed
        file_paths = remove_list_items_in_table(file_paths, "processed_filenames", "processed_filename")

    return file_paths
