import csv
import tools.model.model_classes as merm_model
import tools.utils.log as log
import tools.utils.envutils as env


def run_post_process(package: merm_model.PipelinePackage):
    log.getLogger().info("save noun phrase results to file")
    path = env.config["job_instructions"]["output_folder"]
    np_results = package.any_analysis_dict["noun_phrase_all_groups"]

    for key in np_results:

        analysis = np_results[key]
        if type(analysis) is dict:
            file_name = path +"/" + "noun_phrase" + str(key) + ".csv"
            log.getLogger().info("Saving " + file_name)
            with open(file_name, 'w') as f:
                for rank in analysis.keys():
                    for phrase in analysis[rank]:
                        f.write("%s,%s\n" % (rank, phrase))