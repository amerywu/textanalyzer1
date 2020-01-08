import tools.model.model_classes as merm_model
import tools.elasticsearch_management.elasticsearch_ingestion as ingestor
import tools.utils.log as log
import csv
import time
import json
import uuid


def run_post_process(package: merm_model.PipelinePackage):
    log.getLogger().info("run_post_process")
    env = package.dependencies_dict["env"]
    report_dir = env.config["job_instructions"]["output_folder"]

    csv_list_of_lists = []
    csv_list_of_lists.append(["analysis_type", "major", "topic_id", "terms", "sentence"])
    count = 0
    lda_results = package.any_analysis_dict["lda_all_groups"]
    for analysis_type, analysis_dict in lda_results.items():
        for major, major_dict in analysis_dict.items():
            for topic, sentence_dict in major_dict.items():
                for words, sentences in sentence_dict.items():
                    for sentence in sentences:
                        row_list = [analysis_type, major, topic, words, sentence]
                        csv_list_of_lists.append(row_list)
                        count = count + 1
    log.getLogger().info("LDA corpus saved " + str(count) + " rows.")
    with open(report_dir + "\lda_sentences.csv", "w", newline = "") as f:
        writer = csv.writer(f)
        writer.writerows(csv_list_of_lists)
    log.getLogger().info("LDA Results saved " + str(count) + " rows")

    toes = env.config.getboolean("job_instructions", "output_to_elasticsearch")
    if True == toes:
        _dispatch_to_elastic_search(lda_results, package.any_analysis_dict["provider"])

_current_milli_time = lambda: int(round(time.time() * 1000))


def _index_dict(index_name, src, category, analysis_type, topic, words, sentence):
    adict = {
        '_index': index_name,
        '_id': _id_generator(),
        '_source': _generate_doc(src, category, analysis_type, topic, words, sentence)
    }
    return adict


def _generate_doc(src, category, analysis_type, topic, words, sentence):
    data = {}
    data['category'] = category
    data['analysis_type'] = analysis_type
    data['topic'] = topic
    data['words'] = sentence
    data['sentence'] = words
    data['src'] = src
    data["created"] = _current_milli_time()
    return data



def _id_generator():
    return str(uuid.uuid1())



def _dispatch_to_elastic_search(lda_results, src):
    ingestor.delete_index("corpus_lda")
    count = 0
    ingestor.create_index(_newindex_json(), "corpus_lda")
    for analysis_type, analysis_dict in lda_results.items():
        for major, major_dict in analysis_dict.items():
            for topic, sentence_dict in major_dict.items():
                bulk_list = []
                for words, sentences in sentence_dict.items():
                    for sentence in sentences:
                        count = count + 1
                        if count % 1000 == 0:
                            print(count)
                        bulk_list.append(_index_dict("corpus_lda",src, major, analysis_type, topic, words, sentence))
                ingestor._dispatch_bulk("corpus_lda", bulk_list)
    log.getLogger().info("Dispatched " + str(count) + " rows to ES")


#"format": "yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||epoch_millis"
def _newindex_json():
    idxjson = {
       "settings":{
          "number_of_shards":3,
          "number_of_replicas":2
       },
       "mappings":{
          "properties":{
             "id":{
                "type":"text"
             },
             "src":{
                "type":"text"
             },
              "analysis_type": {
                  "type": "keyword"
              },
             "category":{
                "type":"keyword"
             },
             "topic":{
                "type":"keyword"
             },
              "words": {
                  "type": "text"
              },
             "sentence":{
                "type":"text"
             },
             "created":{
                "type":"date",
                "format" : "epoch_millis"
             }
          }
       }
    }
    return json.dumps(idxjson)








