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


def newindex_json():
    json = {
        "settings": {
            "number_of_shards": 1,
            "analysis": {
                "filter": {
                    "english_stop": {
                        "type": "stop",
                        "stopwords": "_english_"
                    },
                    "english_keywords": {
                        "type": "keyword_marker",
                        "keywords": []
                    },
                    "english_stemmer": {
                        "type": "stemmer",
                        "language": "english"
                    },
                    "english_possessive_stemmer": {
                        "type": "stemmer",
                        "language": "possessive_english"
                    }
                },
                "analyzer": {
                    "english": {
                        "tokenizer": "standard",
                        "filter": [
                            "english_possessive_stemmer",
                            "lowercase",
                            "english_stop",
                            "english_keywords",
                            "english_stemmer"
                        ]
                    }
                }
            }

        },
        "mappings": {
            "_doc": {
                "properties": {
                    "title": {
                        "type": "text",
                        "fields": {
                            "english": {
                                "type":     "text",
                                "analyzer": "english"
                            }
                        }
                    },
                    "id": {
                        "type": "text"

                    },
                    "salient": {
                        "type": "text"
                    },
                    "humanlabels": {
                        "type": "keyword"
                    },
                    "mllabels": {
                        "type": "keyword"
                    },
                    "spacealias": {
                        "type": "text"
                    },
                    "originalurl": {
                        "type": "text"
                    },
                    "webui": {
                        "type": "text"
                    },
                    "content": {
                        "type": "text",
                        "fields": {
                            "english": {
                                "type":     "text",
                                "analyzer": "english"
                            }
                        }
                    },
                    "provider": {
                        "type": "text"
                    },
                    "updated": {
                        "type": "double"
                    },
                    "space": {
                        "type": "text"
                    }
                }
            }
        }
    }
    return json



def run_post_process(package: merm_model.PipelinePackage):
    if env.continue_run():

        tfidf_top_terms: List[List[Tuple[str, float]]] = package.any_analysis()
        _validate_corpus(tfidf_top_terms, package.linked_document_list)
        _create_spaces()
        log.getLogger().info("Corpus size: " + str(len(package.linked_document_list)))
        _iterate_corpus(package)


def _get_providers():
    providers = []
    provider = env.config["extract_instructions"]["provider"]
    if provider == "all":
        providers = str(env.config["extract_instructions"]["all_providers"]).split(",")
    else:
        providers.append(provider)
    return provider



def _create_spaces():
    space_results = es_conn.retrieve_index_registry()
    providers = _get_providers()

    for provider in providers:
        for index_name in space_results:


            log.getLogger().debug("Found index  " + index_name)
            if provider in index_name and not "@" in index_name:
                log.getLogger().debug("Creating  " + str(index_name + index_suffix))
                es_conn.create_and_register_index(index_name + index_suffix, newindex_json() )



def _iterate_corpus(package: merm_model.PipelinePackage):
    count = 0
    try:
        for linked_doc in package.linked_document_list:
            if env.continue_run() == True:
                #for each doc in the corpus
                #split the raw text into sentences. 1 LinkedDocument per sentence
                log.getLogger().debug(str(env.continue_run()))
                doc_by_sentence_list = _split_linked_doc_by_sentence(linked_doc)
                #tokenize and lemmatize the sentences
                doc_uid = linked_doc.uid
                doc_url = linked_doc.ui

                lemmatized_sentences = _lemmatize_sentences(doc_by_sentence_list)
                if len(lemmatized_sentences) > 2000:
                    lemmatized_sentences = lemmatized_sentences[:2000]
                #startmsg = "\n\n" + doc_uid + " | " + linked_doc.ui + " | length: " + str(len(lemmatized_sentences)) + "\n\n"
                #log.getLogger().info(startmsg)

                salient_corpus_map =_generate_partof_docs(package, lemmatized_sentences, doc_uid, doc_url)
                endmsg = "\n\n" + str(count) + ": Dispatching " + str(len(salient_corpus_map)) + " parts from " +doc_url+ ".\n\n"
                log.getLogger().debug(endmsg)
                _generate_json_and_dispatch(salient_corpus_map, 0)
                count = count + 1
                if count % 300 == 0:
                    log.getLogger().info("running " + str(count))
    except Exception as e:
        msg = "\n\nERROR: " +  str(e)
        log.getLogger().error(msg)




def _generate_partof_docs(package: merm_model.PipelinePackage, linked_doc_as_sentence_list:List[merm_model.LinkedDocument], doc_uid:str, doc_url:str):

    def doc_url_as_key(loop_count):
        return doc_url + "$" + str(loop_count)

    #log.getLogger().info("_generate_partof_docs")
    salient_corpus_map = {}
    keep_running = True
    loop_count = 0
    linked_doc_section = linked_doc_as_sentence_list
    while keep_running:
        salient_non_salient_tuple = _break_up_document(package, linked_doc_section)
        #log.getLogger().info("Remaining sentences as proportion of total: " + str(len(salient_non_salient_tuple[1]) / len(linked_doc_as_sentence_list)))
        keep_running = _continue_doc_break_up(salient_non_salient_tuple, len(linked_doc_as_sentence_list), loop_count)
        if keep_running == False:
            #log.getLogger().info("---Part analysis complete.---")
            salient_corpus_map[doc_url_as_key(loop_count)] = (doc_url, salient_non_salient_tuple[0])
            salient_corpus_map[doc_url_as_key(loop_count + 1)] = (doc_url, salient_non_salient_tuple[1])
            keep_running = False
        else:

            linked_doc_section = salient_non_salient_tuple[1]
            salient_corpus_map[doc_url_as_key(loop_count)] = (doc_url, salient_non_salient_tuple[0])
        loop_count =  loop_count + 1

    #log.getLogger().debug("--------_generate_partof_docs done---------")
    return salient_corpus_map

def _continue_doc_break_up(salient_non_salient_tuple, doc_sentence_count, loop_count):
    if len(salient_non_salient_tuple[1]) < 4:
        return False
    elif  len(salient_non_salient_tuple[1]) / doc_sentence_count < 0.2:
        return False
    elif loop_count >3:
        return False
    elif len(salient_non_salient_tuple[0]) == 0:
        return False
    else:
        return True




def _convert_linkeddoclist_to_string(linked_doc_by_sentence_list:List[merm_model.LinkedDocument]):
    plain_string =[]
    for sentence  in linked_doc_by_sentence_list:
        plain_string.append(sentence.raw)
    return " ".join(plain_string)




def _extract_linked_doc_from_list(linked_doc_by_sentence_list:List[merm_model.LinkedDocument]):
    if len(linked_doc_by_sentence_list) > 0:
        return linked_doc_by_sentence_list[0]
    else:
        raise Exception("NOT FOUND: no_sentences_ergo_no_index_name")



def _generate_json_and_dispatch(salient_corpus_map:Dict, retry_count=0):
    try:

        es = es_conn.connectToES()
        total_sentences=0
        for key, value in salient_corpus_map.items():
            sentence_list =  value[1]
            docid = key
            total_sentences = total_sentences + len(sentence_list)
            if len(sentence_list) > 0:
                linked_doc = _extract_linked_doc_from_list(value[1])
                index_name = linked_doc.index_name


                log.getLogger().debug("Dispatching: " + str(docid) + " | " + index_name)
                es.index(index=index_name + index_suffix, doc_type='_doc', id=key, body=_generate_json(linked_doc, _convert_linkeddoclist_to_string(sentence_list)))
    except Exception as e:
        retry_count = retry_count + 1
        msg = "WARN: " +  str(e)

        log.getLogger().error(msg)
        if "time" in msg.lower() and retry_count < 10:
            _generate_json_and_dispatch(salient_corpus_map, retry_count)
        else:
            pass
    #log.getLogger().info("Dispatched " + str(total_sentences))



def _generate_json(linked_doc:merm_model.LinkedDocument, content:str):
    data = {}
    data['humanlabels'] = ''
    data['webui'] = linked_doc.ui
    data['salient'] = ''
    data['provider'] = linked_doc.provider
    data['originalurl'] = linked_doc.source
    data['id'] = linked_doc.uid
    data['title'] = linked_doc.title
    data['content'] = content
    data['space'] = linked_doc.space
    data['mllabels']=''
    data['updated'] = linked_doc.updated
    return json.dumps(data)



def _validate_corpus(tfidf_top_terms: List[List[Tuple[str, float]]], linked_doc_list: List[merm_model.LinkedDocument]):
    docidx = 0
    for terms in tfidf_top_terms:
        linked_doc = linked_doc_list[docidx]
        for word, freq in terms:
            if word not in linked_doc.tokens:
                log.getLogger().error("NOT FOUND " + word)
                raise Exception("NOT FOUND " + word + ". NLP corpus out of sync with source corpus")
        docidx = docidx + 1




def _sentence_contains_one_of(tokens:List[str], toppest_term_tokens:List[str]):
    for toppest_term_token in toppest_term_tokens:

        for token in tokens:
            if token == toppest_term_token:
                return True
    return False

def _break_up_document(package: merm_model.PipelinePackage, doc_by_sentence_list: List[merm_model.LinkedDocument]):

    #log.getLogger().debug("break_up_document. Sentence count " + str(len(doc_by_sentence_list)))
    salient_sentences = []
    not_salient_sentences = []


    if len(doc_by_sentence_list) > 0:
        corpus_doc_sorted = sorted(doc_by_sentence_list[0].corpus_doc, key=itemgetter(1), reverse = True)
        toppest_terms = corpus_doc_sorted[:2]
        toppest_term_tokens = []

        for toppest_term_tuple in toppest_terms:
            toppest_term_tokens.append(package.dict.id2token[toppest_term_tuple[0]])
            #log.getLogger().debug("toppest term: " + str(package.dict.id2token[toppest_term_tuple[0]]) + " : " + str(toppest_term_tuple[1]) )

        for sentence in doc_by_sentence_list:
            tokens = sentence.tokens
            if _sentence_contains_one_of(tokens, toppest_term_tokens):

                salient_sentences.append(sentence)
                #log.getLogger().debug("no slice " + str(sentence.corpus_doc))
            else:
                sentence.corpus_doc = corpus_doc_sorted[2:]
                #log.getLogger().debug("slice " + str(sentence.corpus_doc))
                not_salient_sentences.append(sentence)

    #log.getLogger().info("Total sentences: " + str(len(doc_by_sentence_list))+ ", Salient: " + str(len(salient_sentences)) + ", NotSalient: " + str(len(not_salient_sentences)))
    return (salient_sentences, not_salient_sentences)



def _split_linked_doc_by_sentence(linked_doc: merm_model.LinkedDocument):
     sentence_list = parser.split_linked_doc_by_sentence(linked_doc)
     return sentence_list


def _lemmatize_sentences(doc_by_sentence_list: List[merm_model.LinkedDocument]):
    #for sentence in doc_by_sentence_list:
    #    sentence.raw = parser.clean_string(sentence.raw)

    tokenized_sentences = parser.tokenize(doc_by_sentence_list)
    lemmatized_sentences = parser.lemmatize_tokens(tokenized_sentences, parser.standard_stop_words())

    return lemmatized_sentences
