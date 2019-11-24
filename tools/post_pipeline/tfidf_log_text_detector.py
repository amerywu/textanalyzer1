import json
from operator import itemgetter
from typing import Dict
from typing import List
from typing import Tuple

import tools.model.model_classes as merm_model
import tools.utils.envutils as env
import tools.utils.es_connect as es_conn
import tools.utils.log as log
import tools.utils.text_parsing_utils as parser

index_suffix = "@part_of_doc"



def run_post_process(package: merm_model.PipelinePackage):
    if env.continue_run():

        tfidf_top_terms: List[List[Tuple[str, float]]] = package.any_analysis()

        _validate_corpus(tfidf_top_terms, package.linked_document_list)
        log.getLogger().info("Log Text Detector: Corpus size: " + str(len(package.linked_document_list)))
        _iterate_corpus(package, tfidf_top_terms)
        log.getLogger().info("Log Text Detector: Done " )


def _get_providers():
    providers = []
    provider = env.config["extract_instructions"]["provider"]
    if provider == "all":
        providers = str(env.config["extract_instructions"]["all_providers"]).split(",")
    else:
        providers.append(provider)

    return provider


def _iterate_corpus(package: merm_model.PipelinePackage, tfidf_top_terms: List[List[Tuple[str, float]]]):
    docidx = 0
    for terms in tfidf_top_terms:
        linked_doc:merm_model.LinkedDocument = package.linked_document_list[docidx]
        for word, freq in terms:
            if word not in linked_doc.tokens:
                log.getLogger().error("NOT FOUND " + word)
                raise Exception("NOT FOUND " + word + ". NLP corpus out of sync with source corpus")
            elif len(linked_doc.tokens) > 300:
                if freq > 0.5:
                    _generate_json_and_dispatch(linked_doc)

        docidx = docidx + 1

def _generate_json_and_dispatch(linked_doc:merm_model.LinkedDocument):
    es = es_conn.connectToES()
    index_name = linked_doc.index_name
    log.getLogger().debug("Dispatching: " + str(linked_doc.uid) + " | " + index_name)

    result = es.update(index=index_name, doc_type='_doc', id=linked_doc.uid, body=_generate_json())
    log.getLogger().debug("Dispatched with result " + str(result))

def _generate_json():
    data = {
        "doc" : {
        "mllabels" : "log_type_text"
        }
    }
    #data['mllabels']='log_type_text'
    jsonstring= json.dumps(data)
    log.getLogger().debug(str(jsonstring))
    return jsonstring

def _validate_corpus(tfidf_top_terms: List[List[Tuple[str, float]]], linked_doc_list: List[merm_model.LinkedDocument]):
    docidx = 0
    for terms in tfidf_top_terms:
        linked_doc = linked_doc_list[docidx]
        for word, freq in terms:
            if word not in linked_doc.tokens:
                log.getLogger().error("NOT FOUND " + word)
                raise Exception("NOT FOUND " + word + ". NLP corpus out of sync with source corpus")
        docidx = docidx + 1
