import csv
import tools.model.model_classes as merm_model
import tools.utils.log as log
import tools.utils.envutils as env
import json
import time
import uuid
import tools.elasticsearch_management.elasticsearch_ingestion as ingestor




def run_post_process(package: merm_model.PipelinePackage):
    log.getLogger().info("save noun phrase results to file")
    path = env.config["job_instructions"]["output_folder"]
    np_results = package.any_analysis_dict["noun_phrase_all_groups"]

    count = 0
    for key in np_results:

        analysis = np_results[key]
        if type(analysis) is dict:
            file_name = path +"/" + "noun_phrase" + str(key) + ".csv"
            log.getLogger().info("Saving " + file_name)
            with open(file_name, 'w') as f:
                for rank in analysis.keys():
                    for phrase in analysis[rank]:
                        count = count + 1
                        f.write("%s,%s\n" % (rank, phrase))
    toes = env.config.getboolean("job_instructions", "output_to_elasticsearch")
    log.getLogger().info("Text Rank Results saved " + str(count) + " rows")
    if True == toes:
        _dispatch_to_elastic_search(np_results, package.any_analysis_dict["provider"])

_current_milli_time = lambda: int(round(time.time() * 1000))
# def _current_milli_time():
#     dt = datetime.datetime.now()
#     millis = dt.microsecond
#     print(millis)
#     return millis


def _index_dict(index_name, src, category, sentence):
    adict = {
        '_index': index_name,
        '_id': _id_generator(),
        '_source': _generate_doc(src, category,  sentence)
    }
    return adict


def _generate_doc(src, category, sentence):
    data = {}
    data['category'] = category
    data['sentence'] = sentence
    data['src'] = src
    data["created"] = _current_milli_time()
    return data


def _id_generator( ):
    return str(uuid.uuid1())



def _dispatch_to_elastic_search(np_results, src):
    ingestor.delete_index("corpus_noun_phrase")
    count = 0
    ingestor.create_index(_newindex_json(), "corpus_noun_phrase")
    path = env.config["job_instructions"]["output_folder"]


    for key in np_results:
        bulk_list = []
        analysis = np_results[key]
        if type(analysis) is dict:
            file_name = path + "/" + "noun_phrase" + str(key) + ".csv"
            log.getLogger().info("Saving " + file_name)
            with open(file_name, 'w') as f:
                for rank in analysis.keys():
                    for phrase in analysis[rank]:
                        f.write("%s,%s\n" % (rank, phrase))
                        bulk_list.append(_index_dict("corpus_noun_phrase", src, key, phrase))
        ingestor._dispatch_bulk("corpus_noun_phrase", bulk_list)



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
             "category":{
                "type":"keyword"
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