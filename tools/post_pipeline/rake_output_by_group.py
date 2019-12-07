import csv
import tools.model.model_classes as merm_model
import tools.utils.log as log
import tools.utils.envutils as env


def run_post_process(package: merm_model.PipelinePackage):
    log.getLogger().info("save text rank results to file")
    path = env.config["job_instructions"]["output_folder"]
    rake_results = package.any_analysis_dict["rake_all_groups"]

    for key in rake_results:

        analysis = rake_results[key]
        if "ict" in  type(analysis).__name__:
            file_name = path +"/" + "rake" + str(key) + ".csv"
            with open(file_name, 'w') as f:
                for rank in analysis.keys():
                    for tuple in analysis[rank]:
                        f.write("%s,%s,%s\n" % (rank, tuple[0], tuple[1]))