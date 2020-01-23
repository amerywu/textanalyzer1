import tools.pipeline_framework.post_process_factory as post_process
import tools.model.model_classes as merm_model
from datetime import datetime
import tools.utils.envutils as env
import tools.utils.log as log

######################################################
# This script functions as programmatic configuration of a pipeline.
# Other scripts in this package focus on a specific genre of functionality.
# Each class in a script performs a single function.
# run_pipeline starts with a Pandas DataFrame of data retrieved from ES.
# The data is then converted PipelinePackage.
# All pipeline functions then manipulate PipelinePackage, e.g., by building a Gensim corpus
# or (subsequently) using the corpus to run analyses such as LDA, TF-IDF etc)

_gensim_lda_steps = [
    (10, "SaveDfAsCsv"),
    (15, "DataframeToListOfLists"),
    (20, "ListOfListsToGensimCorpora"),
    (22, "GensimLDA")
]
_gensim_lda_by_subset_steps = [
    (10, "DataframeToListOfLists"),
    (30, "GensimLdaGrouped_SubPipe"),
]
_lda_topic_comparator_steps = [
    (10, "TopicLoader"),
    (11, "TopicComparator")

]

_sklearn_lda_steps = [
    (10, "DataframeToListOfLists"),
    (20, "LinkedDocListToScikitLDACorpus"),
    (30, "ScikitLDA"),
    (40, "SciKitLDAReport"),
]



_text_rank = [
    (10, "TextCleaner_DF"),
    (20, "DataframeToListOfLists"),
    (35, "LinkedDocCorpusWordCount"),
    (30, "TextRankGrouped_SubPipe"),
    (40, "GensimLdaGrouped_SubPipe"),
    (50, "TextRankResultsToLinkedDocList"),
    (60, "GensimLdaGrouped_SubPipe"),
    (63, "RakeResultsToLinkedDocList"),
    (65, "GensimLdaGrouped_SubPipe"),
    (67, "NounPhraseResultsToLinkedDocList"),
    (70, "GensimLdaGrouped_SubPipe"),
    (80, "GensimSentenceFinder"),
]

_category_prediction = [
    (10, "DataframeToListOfLists"),
    (11, "RemoveDuplicateDocs"),
    #(12, "MergeCategories1"),
    (14, "CountBySpaceAndGroup"),
    (15, "ExcludeByGroup"),
   # (20, "ExcludeBySpace"),
    (30, "CountBySpaceAndGroup"),
    (40, "StopWord_SubPipe"),
    (45, "CountBySpaceAndGroup"),
    (50, "LinkedDocListToScikitRFCorpus"),
    (60, "ScikitRF"),
    (65, "ScikitNearMisses"),
    (70, "ScikitSentenceFinder"),
    (75, "ScikitPrettyConfusion"),

    ]

_category_prediction_ld = [
    (1, "EsExtract"),
    (10, "DataframeToListOfLists"),
    (11, "RemoveDuplicateDocs"),
    #(12, "MergeCategories1"),
    (14, "CountBySpaceAndGroup"),
    #(15, "ExcludeByGroup"),
    (20, "ExcludeBySpace"),
    (30, "CountBySpaceAndGroup"),
    (40, "StopWord_SubPipe"),
    (45, "CountBySpaceAndGroup"),
    (50, "FilterTokensByCount"),
    (60, "EvenBySpace"),
    (80, "CountBySpaceAndGroup"),
    # (85, "ScikitNearMisses"),
    # (90, "ScikitSentenceFinder"),
    # (95, "ScikitPrettyConfusion"),
    # (100, "Reset"),
    (120, "TokensToDoc"),
    (145, "CountBySpaceAndGroup"),
    (150, "LinkedDocListToScikitRFCorpus"),
    (180, "ScikitLinearDiscriminantAnalysis"),
    (185, "ScikitNearMisses"),
    (190, "ScikitSentenceFinder"),
    (195, "ScikitPrettyConfusion"),
    (199, "Loop")
]


_save_as_csv = [
    (10, "SaveDfAsCsv")
]


_job_integrity_analysis = [
    (10, "JobDfAnalysis"),
    (20, "AreasOfStudyDfAnalysis"),
    (30, "SaveDfAsCsv")
]

_group_by_column = [
    (10, "DfGroupByAnalysis")
]

_rake = [
    (10, "RakeAnalysis")
]


def pick_pipeline():
    pipeline_name = env.config["pipeline_instructions"]["pipeline_name"]
    log.getLogger().info(pipeline_name)

    if pipeline_name == "gensim_lda":
        return _gensim_lda_steps
    elif pipeline_name == "gensim_lda_by_subset":
        return _gensim_lda_by_subset_steps
    elif pipeline_name == "sklearn_lda":
        return _sklearn_lda_steps
    elif pipeline_name == "lda_topic_comparator":
        return _lda_topic_comparator_steps
    elif pipeline_name == 'save_as_csv':
        return _save_as_csv
    elif pipeline_name == '_job_integrity_analysis':
        return _job_integrity_analysis
    elif pipeline_name == '_group_by_column':
        return _group_by_column
    elif pipeline_name == '_rake':
        return _rake
    elif pipeline_name == '_text_rank':
        return _text_rank
    elif pipeline_name == "_category_prediction":
        return _category_prediction
    elif pipeline_name == "_category_prediction_ld":
        return _category_prediction_ld
    else:
        log.getLogger().warning(str(pipeline_name) + " is invalid. Please configure tools.ini and create a relevant list of steps within this script")
        return []


def step_through(package:merm_model.PipelinePackage, pipeline_steps, log_string):

    factory = package.dependencies_dict["pipe_process"].PipelineFactory()
    for step_tuple in pipeline_steps:
        if env.continue_run() == True:
            package = factory.next_step(step_tuple[1], package)
            log_string = log_string + "\n\n------------\n\n" + step_tuple[1]+ "\n\n"+package.stage_log()
        else:
            log.getLogger().warning("Continue run is FALSE")
        package.log_stage(log_string)
    return package


def run_pipeline(package:merm_model.PipelinePackage):
    log.getLogger().warning("------- STARTING PIPELINE -------")
    env = package.dependencies_dict["env"]
    report_dir = env.config["job_instructions"]["output_folder"]
    provider = env.config["extract_instructions"]["provider"]
    pipeline_name = env.config["pipeline_instructions"]["pipeline_name"]
    queryvalue = env.config["extract_instructions"]["query_value"]
    dt = datetime.now()
    suffix = str(dt.microsecond)[-4:]
    file_name = provider + "_" + pipeline_name + "_" + queryvalue + "_" + suffix +".txt"



    #create factory


    # specify steps
    pipeline_steps = pick_pipeline()
    log.getLogger().info(str(pipeline_steps))


    pipeline_steps.sort(key=lambda tup: tup[0])
    log_string = ""

    # ...and we're off to the races :)
    package = step_through(package, pipeline_steps, log_string)
    if "current_loop" in package.any_inputs_dict.keys():
        current_loop = package.any_inputs_dict["current_loop"]
        while current_loop < package.any_inputs_dict["loop_count"]:
            current_loop = package.any_inputs_dict["current_loop"]
            package = step_through(package, pipeline_steps, package.stage_log())



    env.overwrite_file(report_dir + "/" + file_name, package.stage_log())
    log.getLogger().info("------- PIPELINE COMPLETED -------")

    # Post pipeline; This is where the data is no longer changing. Rather, the data is ready
    # for functional application.
    log.getLogger().warning("------- POST PROCESS APPLICATION -------")
    if env.continue_run() == True:
        post_process.triage(package)
