
import tools.model.model_classes as merm_model
import tools.post_pipeline.page_view_update as page_view_update
import tools.post_pipeline.tfidf_partof_doc_generator as tfidf_breakout
import tools.post_pipeline.tfidf_log_text_detector as log_detector
import tools.post_pipeline.gensim_lda_report_by_subset as gensim_lda_report_by_subset
import tools.post_pipeline.gensim_lda_report as gensim_lda_report
import tools.post_pipeline.gensim_lda_report_topic_similarity as gensim_similarity_report
import tools.post_pipeline.save_dictionaries_to_file as save_dictionaries_to_file
import tools.post_pipeline.major_analysis as major_analysis
import tools.post_pipeline.rake as rake


import tools.utils.envutils as env
import tools.utils.log as log


def triage(package:merm_model.PipelinePackage):
    instructions = env.config["pipeline_instructions"]["post_process"]

    instruction_list = instructions.split(",")
    for instruction in instruction_list:


        if instruction == "tfidf_partof_sentence_breakout":
            tfidf_breakout.run_post_process(package)
        elif instruction == "page_views_confluence":
            page_view_update.run_post_process(package)
        elif instruction == "gensim_lda_report_by_subset":
            gensim_lda_report_by_subset.run_post_process(package)
        elif instruction == "gensim_lda_report":
            gensim_lda_report.run_post_process(package)
        elif instruction == "tfidf_log_text_detector":
            log_detector.run_post_process(package)
        elif instruction == "gensim_lda_report_topic_similarity":
            gensim_similarity_report.run_post_process(package)
        elif instruction == "save_dictionaries_to_file":
            save_dictionaries_to_file.run_post_process(package)

        elif instruction == "major_analysis":
            major_analysis.run_post_process(package)
        elif instruction == "rake":
            rake.run_post_process(package)
        elif instruction == "none":
            log.getLogger().info("Nothing to do. No post-process assigned.")
