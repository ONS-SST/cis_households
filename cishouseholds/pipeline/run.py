import os
from datetime import datetime

import yaml

import cishouseholds.pipeline.bloods_delta_ETL  # noqa: F401
import cishouseholds.pipeline.survey_responses_version_2_ETL  # noqa: F401
import cishouseholds.pipeline.swab_delta_ETL  # noqa: F401
from cishouseholds.pipeline.pipeline_stages import pipeline_stages


def run_from_config():
    """
    Reads yaml config file containing variables (run, function and resource path) per ETL function
    requires setting of PIPELINE_CONFIG_LOCATION environment var with file path of output
    """
    with open(os.environ["PIPELINE_CONFIG_LOCATION"]) as fh:
        config = yaml.load(fh, Loader=yaml.FullLoader)
    for ETL in config["stages"]:
        if ETL["run"]:
            output_df = pipeline_stages[ETL["function"]](ETL["resource_path"])
            output_df.toPandas().to_csv(
                "{}/{}_output_{}.csv".format(
                    config["csv_output_path"], ETL["function"], datetime.now().strftime("%y%m%d_%H%M%S")
                ),
                index=False,
            )


if __name__ == "__main__":
    run_from_config()
