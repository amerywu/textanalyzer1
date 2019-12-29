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



def _get_docs_list(package: merm_model.PipelinePackage, idxname):
    if type(package.linked_document_list) is list:
        return package.linked_document_list
    elif idxname in package.linked_document_list.keys():
        return package.linked_document_list[idxname]
    elif "linked_docs" in package.cache_dict.keys():
        if idxname in package.cache_dict["linked_docs"].keys():
            return package.cache_dict["linked_docs"][idxname]
    else:
        return []


def run_post_process(package: merm_model.PipelinePackage):
    log.getLogger().info("run_post_process")
    report_dir = package.dependencies_dict["env"].config["job_instructions"]["output_folder"]
    csv_list_of_lists = []
    csv_list_of_lists.append(["index_name", "topic_id", "term", "weight"])
    report_sentences = env.config.getboolean('ml_instructions', 'gensim_lda_report_sentence_level')
    analysis_key = gensim_grouped.analysis_output_label()
    minimum_doc_count = package.dependencies_dict["env"].config.getint('ml_instructions', 'minimum_doc_count')
    for idxname, topicdict in package.any_analysis_dict[analysis_key].items():
        report_for_index = "\n\n\n+++++++++++++++++++\n\nReport for " + idxname +"\n\n"
        docs_list = _get_docs_list(package, idxname)
        if report_sentences == True:
            corpus_as_sentences = break_corpus_as_sentences(docs_list)
        report_for_index += "Corpus Size: " + str(len(docs_list)) + "\n"

        if len(docs_list) > minimum_doc_count:
            for topicid, topiclist in topicdict.items():
                report_for_index +="\n\nTOPIC:" + str(topicid) +"\n"

                for entry in topiclist:
                    report_for_index += str(entry[0])
                    report_for_index += "\t\t\t"
                    report_for_index += str(entry[1])
                    report_for_index += "\n"
                    csv_list_of_lists.append([idxname, topicid, entry[0], entry[1]])
                if report_sentences == True:
                    salient_sentences = find_salient_sentences(topiclist,corpus_as_sentences)
                    report_for_index += "\n\nSALIENT_SENTENCES\n"
                    for sentence in salient_sentences:
                        report_for_index += sentence + "\n"

        log.getReportLogger().info(report_for_index)
    _save_topic_model(package)
    _save_csv(csv_list_of_lists, "lda_analysis_by_subset", report_dir)

def _save_topic_model(package: merm_model.PipelinePackage):
    words_for_topic = []
    words_for_topic.append(["source", "topic", "term", "weight"])
    report_dir = package.dependencies_dict["env"].config["job_instructions"]["output_folder"]
    for subset_name, subset_model in package.model.items():
        for index, topic in subset_model.show_topics(formatted=False, num_words=50):
            # print('Topic: {} \nWords: {}'.format(index, [w[0] for w in topic]))
            for w in topic:
                msg = str(index) + ":" + str(w)
                log.getLogger().info(msg)
                words_for_topic.append([ subset_name, index, w[0], w[1]])
    _save_csv( words_for_topic, "lda_topics_by_subset_raw", report_dir)


def _save_csv(list_of_lists, file_name,report_dir):

    with open(os.path.join(report_dir, '') + file_name + str(time.time()) + ".csv", 'w', newline = '') as csvFile:
        writer = csv.writer(csvFile)
        writer.writerows(list_of_lists)

    csvFile.close()

def break_corpus_as_sentences(docs_list:List[merm_model.LinkedDocument]):
    corpus_as_sentences = []
    for doc in docs_list:
        corpus_as_sentences = corpus_as_sentences + parser.split_linked_doc_by_sentence(doc)
    corpus_as_sentences = parser.tokenize(corpus_as_sentences)
    corpus_as_sentences = parser.lemmatize_tokens(corpus_as_sentences, parser.standard_stop_words())
    return corpus_as_sentences



def find_salient_sentences(topiclist:List[Tuple[str,int]],corpus_as_sentences:List[merm_model.LinkedDocument]):
    top_words_indices = [0,1,2,3,4,5,6]
    raw_sentences = []

    bottomidx = 0
    while (bottomidx < len(top_words_indices)):
        topidx = bottomidx + 2
        while topidx < len(top_words_indices):
            words = [topiclist[bottomidx][0],topiclist[topidx -1][0], topiclist[topidx][0]]
            raw_sentences = raw_sentences + words_in_sentence_list(words,corpus_as_sentences)
            topidx = topidx + 1
        bottomidx = bottomidx + 1

    if len(raw_sentences) < 5:
        bottomidx = 0
        while (bottomidx < len(top_words_indices)):
            topidx = bottomidx + 1
            while topidx < len(top_words_indices):
                words = [topiclist[bottomidx][0], topiclist[topidx][0]]
                raw_sentences = raw_sentences + words_in_sentence_list(words, corpus_as_sentences)
                topidx = topidx + 1
            bottomidx = bottomidx + 1


    return raw_sentences


def words_in_sentence_list(tokens:List[str],corpus_as_sentences:List[merm_model.LinkedDocument]):
    raw_sentences=[]
    for sentence in corpus_as_sentences:
        all_tokens_found=True
        for token in tokens:
            if not token in sentence.tokens:
                all_tokens_found = False
        if all_tokens_found == True:
            raw_sentences.append(str(tokens) +" ==> "+sentence.raw)
    return raw_sentences







