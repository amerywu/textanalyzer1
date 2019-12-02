from typing import List
from typing import Dict
import pandas as pd
from pandas import DataFrame

import tools.model.model_classes as merm_model
import tools.utils.dfutils as dfu

import tools.utils.es_connect as es_conn
import tools.utils.log as log


def _dev_bool(dependencies_dict:Dict):
    dev_mode = dependencies_dict["env"].config["pipeline_instructions"]["testenv"]
    dev_bool = (dev_mode == "true")
    return dev_bool

def _dev_limit(dependencies_dict:Dict):

    dev_limit = dependencies_dict["env"].config["pipeline_instructions"]["testenv_doc_process_count"]

    limit = 10000000
    if True == _dev_bool(dependencies_dict):
        limit = int(str(dev_limit))

    return limit
# Retrieves space names from ES metadata index. Iteratating by space, retrieves all documents in each space.
# Each space retrieval involves a single Pandas dataframe. These are then concatenated into a single DataFrame for
# starting the pipeline
def _extract(es, pipe, dependencies_dict:Dict):

    provider = dependencies_dict["env"].config["extract_instructions"]["provider"]
    msg = "\n\n\n================\nExtracting from " + str(provider)
    log.getLogger().warning(msg)
    if provider == "all":
        _extract_from_all_providers(es, pipe, dependencies_dict)
    elif provider == "none":
        _enter_pipeline(
            merm_model.PipelinePackage(None, None, None, None, None, dependencies_dict), pipe)
    else:
        _extract_from_one_provider(es, provider, pipe, dependencies_dict)


def _enter_pipeline(package:merm_model.PipelinePackage, pipe):
    pipe(package)


def _extract_from_all_providers(es, pipe, dependencies_dict):
    providers=dependencies_dict["env"].config["extract_instructions"]["all_providers"]
    providers_list = providers.split(",")
    log.getLogger().debug("Extracting from all providers: " + str(providers_list))
    for provider in providers_list:
        _extract_from_one_provider(es,provider, pipe, dependencies_dict)


def _extract_from_one_provider(es, provider, pipe, dependencies_dict:Dict):
    msg = "\n\n-------------------------\nPROVIDER: " + str(provider) + "\n---------------------\n\n"
    log.getLogger().warning(msg)
    ignore_indices = dependencies_dict["env"].config["extract_instructions"]["ignore_indices"]
    ignore_indices_list = ignore_indices.split(",")
    indices = es_conn.retrieve_index_registry()

    limit = _dev_limit(dependencies_dict)
    count = 0

    df_per_space_list: List[DataFrame] = []
    for index_name in indices:
        if "@" in index_name:
            continue
        if index_name in ignore_indices_list:
            continue
        if count > limit:
            break
        if provider in index_name  :
            df = _retrieve_index_content(es, index_name, provider, limit)
            if not df.empty:
                log.getLogger().debug("Retrieved " + index_name + ": row count " + str(df.shape))
                count = count + df.shape[0]
                df_per_space_list.append(df)

    if len(df_per_space_list) > 0:
        complete_corpus_df = pd.concat(df_per_space_list, ignore_index=True)
        if True == _dev_bool(dependencies_dict):
            complete_corpus_df = complete_corpus_df.head(limit)
        #log.getLogger().info("\n\nExtraction Complete. Document count = " + str(complete_corpus_df[:5]))
        log.getLogger().info("complete_corpus_df shape: " + str(complete_corpus_df.shape))
        dfu.col_names(df,"complete_corpus_df")
        msg = "\n\n>>>>>>>>>>>>>>   Entering Pipeline For  " + str(provider) + ">>>>>>>>>>\n\n"
        log.getLogger().info(msg)
        analysis_dict = {}
        analysis_dict["provider"] = provider
        _enter_pipeline(merm_model.PipelinePackage(None, complete_corpus_df,None,None,analysis_dict,dependencies_dict), pipe)

#Starting point for extraction purpose (pass pipeline in as function to avoid mutually dependent import statements)
def initiate_extraction(pipe, dependencies_dict):
    es = es_conn.connectToES()
    return _extract(es, pipe, dependencies_dict)

def scrolled_search(es, index_name, limit):
    log.getLogger().info(index_name)
    total = 0
    page_list = []
    page = es.search(
        index= index_name,
        scroll='2m',
        size=1000,
        body={
            'query': {
                'match_all': {}
            }
        })
    sid = page['_scroll_id']
    scroll_size = page['hits']['total']['value']
    page_list.append(page)

    # Start scrolling
    log.getLogger().info("scrolling")
    while (scroll_size > 0):
        total = total + scroll_size
        if total > limit:
            break
        page = es.scroll(scroll_id=sid, scroll='2m')
        # Update the scroll ID
        sid = page['_scroll_id']
        # Get the number of results that we returned in the last scroll
        scroll_size = len(page['hits']['hits'])
        #print ("scroll size: " + str(scroll_size))
        page_list.append(page)
        log.getLogger().info("still scrolling")
    return page_list


def _process_row(content,provider):
    if provider == "job":
        return _process_job_row(content)
    elif provider == "possible-phrase":
        return _process_pp_row(content)
    else:
        raise Exception("Unknown Provider " + provider)

#Retrieves all documents in an ES Index. Each index contains content from one confluence space
def _retrieve_index_content(es, index_name, provider, limit):

    rows_list = []
    log.getLogger().debug("Retrieving content for " + index_name)
    space_content_page_list = scrolled_search(es, index_name, limit)
    count = 0
    for space_content_result in space_content_page_list:
        for content in space_content_result['hits']['hits']:
            try:
                row = _process_row(content, provider)
                rows_list.append(row)
                count = count + 1
            except:
                log.getLogger().error(str(index_name) + " failed to get row: " + str(count))
                log.getLogger().error("maybe add this index to tools.ini ignore list?")
                pass
        log.getLogger().info("Extracted "+ str(count))
        if count > limit:
            break

    return pd.DataFrame(rows_list)



def _process_pp_row(content):
    row = {
        "possible-phrase": content['_source']['possible-phrase'],
        "token-type": content['_source']['token-type']
    }
    return row


def _process_job_row(content):
    row = {
        "areasOfStudy": content['_source']['areasOfStudy'],
        "certification": content['_source']['certification'],
        "contentType": content['_source']['contentType'],
        "id": content['_id'],
        "created": content['_source']['created'],
        "databaseDescriptor": content['_source']['databaseDescriptor'],
        "degreeLevel": content['_source']['degreeLevel'],
        "detailLevel": content['_source']['detailLevel'],
        "jobFinal": content['_source']['jobFinal'],
        "indexname": content['_index'],
        "jobTitles": content['_source']['jobTitles'],
        "location": content['_source']['location'],
        "majorFinal": content['_source']['majorFinal'],
        "qualifications": content['_source']['qualifications'],
        "region": content['_source']['region'],
        "salaryEnd": content['_source']['salaryEnd'],
        "salaryStart": content['_source']['salaryStart'],
        "skills": content['_source']['skills'],
        "relatedMajors": content['_source']['relatedMajors'],
        "workSkills": content['_source']['workSkills'],
        "yearsExperience": content['_source']['yearsExperienceAsInt'],
    }
    return row

def retrieve_slack_channel_registry(es):

    rows_list = []
    log.getLogger().debug("Retrieving content for slack$$channel_registry@")


    space_content_page_list = scrolled_search(es, "slack$$channel_registry@")
    count = 0

    for space_content_result in space_content_page_list:
        for content in space_content_result['hits']['hits']:
            rows_list.append((content['_source']['channelid'],content['_source']['channelname']))
    return dict(rows_list)

