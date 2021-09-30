import os
from datetime import datetime

import yaml

from cishouseholds.pipeline.a_test_ETL import a_test_ETL  # noqa: F401
from cishouseholds.pipeline.bloods_delta_ETL import bloods_delta_ETL  # noqa: F401
from cishouseholds.pipeline.declare import ETL_scripts
from cishouseholds.pipeline.declare import Merge_scripts
from cishouseholds.pipeline.survey_responses_version_2_ETL import survey_responses_version_2_ETL  # noqa: F401
from cishouseholds.pipeline.swab_delta_ETL import swab_delta_ETL  # noqa: F401


def run_from_config():
    """
    reads yaml config file containing variables (run, function and resource path) per ETL function
    requires setting of PIPELINE_CONFIG_LOCATION environment var with file path of output
    """
    with open(os.environ["PIPELINE_CONFIG_LOCATION"]) as fh:
        config = yaml.load(fh, Loader=yaml.FullLoader)
    for ETL in config["stages"]:
        if ETL["run"]:
            output_df = ETL_scripts[ETL["function"]](ETL["resource_path"])
            for function_name, merge_function in Merge_scripts.items():
                if any(x in function_name for x in ["blood", "swab"]):
                    print("running...", function_name)
                    output_df = merge_function(output_df)
            output_df.toPandas().to_csv(
                "{}/{}_output_{}.csv".format(config["csv_output_path"], ETL["function"], datetime.now()), index=False
            )


if __name__ == "__main__":
    os.environ["PIPELINE_CONFIG_LOCATION"] = "C:/code/cis_households/cishouseholds/pipeline/config.yaml"
    run_from_config()
