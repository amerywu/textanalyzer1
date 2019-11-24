import csv
import tools.model.model_classes as merm_model
import tools.utils.log as log
import tools.utils.envutils as env


def run_post_process(package: merm_model.PipelinePackage):
    log.getLogger().info("save dictionaries to file")
    path = env.config["job_instructions"]["output_folder"]

    for key in package.any_analysis_dict:

        analysis = package.any_analysis_dict[key]
        if "ict" in  type(analysis).__name__:
            file_name = path +"/"+key+".csv"
            with open(file_name, 'w') as f:
                for k in analysis.keys():
                    f.write("%s,%s\n" % (k, analysis[k]))