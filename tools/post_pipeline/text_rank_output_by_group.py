import csv
import tools.model.model_classes as merm_model
import tools.utils.log as log
import tools.utils.envutils as env


def run_post_process(package: merm_model.PipelinePackage):
    log.getLogger().info("save text rank results to file")
    path = env.config["job_instructions"]["output_folder"]
    text_rank_results = package.any_analysis_dict["text_rank_all_groups"]

    for key in text_rank_results:

        analysis = text_rank_results[key]["text_rank"]
        if "ict" in  type(analysis).__name__:
            file_name = path +"/" + "TextRank_" + key + ".csv"
            with open(file_name, 'w') as f:
                for k in analysis.keys():
                    for sentence in analysis[k]:
                        f.write("%s,%s\n" % (k, sentence))