from operator import itemgetter
from typing import Dict
from typing import List
from typing import Tuple
import os
import tools.model.model_classes as merm_model
import tools.utils.envutils as env
import tools.utils.es_connect as es_conn
import tools.utils.log as log
import tools.utils.text_parsing_utils as parser
import tools.text_sub_pipeline.gensim_lda_grouped as gensim_grouped
import csv
import time
import re


def run_post_process(package: merm_model.PipelinePackage):
    log.getLogger().info("run_post_process")
    env = package.dependencies_dict["env"]
    report_dir = env.config["job_instructions"]["output_folder"]

    csv_list_of_lists = []
    csv_list_of_lists.append(["analysis_type", "major", "topic_id", "terms", "sentence"])
    for analysis_type, analysis_dict in package.any_analysis_dict["lda_all_groups"].items():
        for major, major_dict in analysis_dict.items():
            for topic, sentence_dict in major_dict.items():
                for words, sentences in sentence_dict.items():
                    for sentence in sentences:
                        row_list = [analysis_type, major, topic, words, sentence]
                        csv_list_of_lists.append(row_list)

    with open(report_dir + "\lda_sentences.csv", "w", newline = "") as f:
        writer = csv.writer(f)
        writer.writerows(csv_list_of_lists)









