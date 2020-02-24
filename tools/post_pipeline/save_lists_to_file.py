import csv
import tools.model.model_classes as merm_model
import tools.utils.log as log
import tools.utils.envutils as env
from datetime import datetime

def _separator_actually(separator):
    if separator == "space":
        return " "
    elif separator == "tab":
        return "\t"
    else:
        return ","

def run_post_process(package: merm_model.PipelinePackage):
    log.getLogger().info("save lists to file")
    path = env.config["job_instructions"]["output_folder"]
    separator_code = env.config["job_instructions"]["csv_separator"]
    separator = _separator_actually(separator_code)

    dt = datetime.now()
    suffix = str(dt.microsecond)[-4:]

    for key in package.any_analysis_dict:

        analysis = package.any_analysis_dict[key]
        if  type(analysis) is list:
            if type(analysis[0]) is list:
                file_name = path + "/" + key + "_" + suffix + ".csv"
                if len(analysis[0]) > 0 and type(analysis[0][0]) is list:
                    with open(file_name, "w", newline="") as f:
                        for rows in analysis:
                            writer = csv.writer(f, delimiter = separator )
                            writer.writerows(rows)
                else:
                    with open(file_name, "w", newline="") as f:
                        writer = csv.writer(f, delimiter = separator)
                        writer.writerows(analysis)