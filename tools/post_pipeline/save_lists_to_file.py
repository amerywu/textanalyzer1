import csv
import tools.model.model_classes as merm_model
import tools.utils.log as log
import tools.utils.envutils as env
from datetime import datetime

def run_post_process(package: merm_model.PipelinePackage):
    log.getLogger().info("save lists to file")
    path = env.config["job_instructions"]["output_folder"]
    dt = datetime.now()
    suffix = str(dt.microsecond)[-4:]

    for key in package.any_analysis_dict:

        analysis = package.any_analysis_dict[key]
        if  type(analysis) is list:
            if type(analysis[0]) is list:
                file_name = path +"/"+key+"_" + suffix+".csv"
                with open(file_name, "w", newline="") as f:
                    writer = csv.writer(f)
                    writer.writerows(analysis)