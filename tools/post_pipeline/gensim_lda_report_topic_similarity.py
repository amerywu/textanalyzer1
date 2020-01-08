import os
import tools.model.model_classes as merm_model
import tools.utils.envutils as env
import tools.utils.log as log
import csv
import time

def run_post_process(package: merm_model.PipelinePackage):
    log.getLogger().info("run_post_process: gensim_lda_report_topic_similarity")
    csv_list_of_lists = []
    csv_list_of_lists.append(["space_topic", "terms", "similarity", "related"])
    similarity_dict = package.any_analysis_dict["similarity_dict"]
    for source, little_dic in similarity_dict.items():
        terms = little_dic["terms"]
        spaces = little_dic["spaces"]
        for space in spaces:
            csv_list_of_lists.append([source,terms,space[0], space[1]])

    _save_csv(csv_list_of_lists)

def _save_csv(list_of_lists):
    env.mkdirInCwd("output")
    with open(os.path.join("output", '') + "lda_topic_similarity" + str(time.time()) + ".csv", 'w') as csvFile:
        writer = csv.writer(csvFile)
        writer.writerows(list_of_lists)
    csvFile.close()









