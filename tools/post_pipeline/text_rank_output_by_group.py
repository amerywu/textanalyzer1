import csv
import tools.model.model_classes as merm_model
import tools.utils.log as log
import tools.utils.envutils as env
import tools.elasticsearch_management.elasticsearch_ingestion as ingestor
import json
import time
import uuid

def run_post_process(package: merm_model.PipelinePackage):
    log.getLogger().info("save text rank results to file")
    path = env.config["job_instructions"]["output_folder"]
    text_rank_results = package.any_analysis_dict["text_rank_all_groups"]
    count = 0
    for key in text_rank_results:

        analysis = text_rank_results[key]
        if "ict" in  type(analysis).__name__:
            file_name = path +"/" + "TextRank_" + str(key) + ".csv"
            log.getLogger().info("Saving "+ file_name)
            with open(file_name, 'w') as f:
                for k in analysis.keys():
                    for sentence in analysis[k]:
                        count = count + 1
                        f.write("%s,%s\n" % (k, sentence))
    toes = env.config.getboolean("job_instructions","output_to_elasticsearch")
    log.getLogger().info("Text Rank Results saved " + str(count) + " rows")
    if True == toes:
        _dispatch_to_elastic_search(text_rank_results, package.any_analysis_dict["provider"])

_current_milli_time = lambda: int(round(time.time() * 1000))
# def _current_milli_time():
#     dt = datetime.datetime.now()
#     millis = dt.microsecond
#     print(millis)
#     return millis


def _index_dict(index_name, src, category, rank, sentence):
    adict = {
        '_index': index_name,
        '_id': _id_generator(),
        '_source': _generate_doc(src, category, rank, sentence)
    }
    return adict


def _generate_doc(src, category, rank, sentence):
    data = {}
    data['category'] = category
    data['rank'] = rank
    data['sentence'] = sentence
    data['src'] = src
    data["created"] = _current_milli_time()
    return data


def _id_generator( ):
    return str(uuid.uuid1())


def _dispatch_to_elastic_search(text_rank_results, src):
    count = 0
    ingestor.delete_index("corpus_text_rank")
    ingestor.create_index(_newindex_json(), "corpus_text_rank")
    for category in text_rank_results:
        analysis = text_rank_results[category]
        if type(analysis) is dict:

            for rank in analysis.keys():
                bulk_list = []

                for sentence in analysis[rank]:
                    count = count + 1
                    if count % 1000 == 0:
                        print(count)
                    bulk_list.append(_index_dict("corpus_text_rank", src, category,rank, sentence))
                    #ingestor._dispatch("corpus_text_rank", _id_generator(),json)
                log.getLogger().info("dispatching")
                ingestor._dispatch_bulk("corpus_text_rank", bulk_list)
    log.getLogger().info("Dispatched "+ str(count) + " rows to ES")



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
             "category":{
                "type":"keyword"
             },
             "rank":{
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