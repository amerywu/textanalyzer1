import tools.elasticsearch_management.elasticsearch_ingestion as ingestor
import tools.model.model_classes as merm_model
import tools.utils.log as log
import tools.utils.envutils as env
import time
import hashlib
import json
import uuid

def run_post_process(package: merm_model.PipelinePackage):
    log.getLogger().info("save rake results to file")
    path = env.config["job_instructions"]["output_folder"]
    rake_results = package.any_analysis_dict["rake_all_groups"]
    src = package.any_analysis_dict["provider"]
    count = 0
    for key in rake_results:
        analysis = rake_results[key]
        if "ict" in  type(analysis).__name__:
            file_name = path +"/" + "rake" + str(key) + ".csv"
            log.getLogger().info("Saving " + file_name)
            with open(file_name, 'w') as f:
                for rank in analysis.keys():
                    for tuple in analysis[rank]:
                        count = count + 1
                        f.write("%s,%s,%s\n" % (rank, tuple[0], tuple[1]))
    toes = env.config.getboolean("job_instructions","output_to_elasticsearch")
    log.getLogger().info("Rake Results saved " + str(count) + " rows")
    if True == toes:
        _dispatch_to_elastic_search(rake_results, package.any_analysis_dict["provider"])

_current_milli_time = lambda: int(round(time.time() * 1000))


def _generate_json(src, category, score, rank, sentence):
    data = {}
    data['category'] = category
    data['rank'] = rank
    data['score'] = score
    data['sentence'] = sentence
    data['src'] = src
    data["created"] = _current_milli_time()
    return json.dumps(data)


def _id_generator():
    return uuid.uuid1()

def _index_dict(index_name, src, category, score, rank, sentence):
    adict = {
        '_index': index_name,
        '_id': _id_generator(),
        '_source': _generate_doc(src, category, score, rank, sentence)
    }
    return adict


def _generate_doc(src, category, score, rank, sentence):
    data = {}
    data['category'] = category
    data['rank'] = rank
    data['score'] = score
    data['sentence'] = sentence
    data['src'] = src
    data["created"] = _current_milli_time()
    return data



def _id_generator():
    return str(uuid.uuid1())


def _dispatch_to_elastic_search(text_rank_results, src):
    ingestor.delete_index("corpus_rake")
    count = 0
    ingestor.create_index(_newindex_json(), "corpus_rake")
    for category in text_rank_results:
        analysis = text_rank_results[category]
        if type(analysis) is dict:
            bulk_list = []
            for rank in analysis.keys():
                for sentence_tuple in analysis[rank]:
                    count = count + 1
                    if count % 1000 == 0:
                        print(count)
                    bulk_list.append(_index_dict("corpus_rake", src, category, sentence_tuple[1], rank, sentence_tuple[0]))
            log.getLogger().info("Dispatching " + str(len(bulk_list)) + " rows to ES")
            ingestor._dispatch_bulk("corpus_rake", bulk_list)



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
             "phrase":{
                "type":"text"
             },
              "score": {
                  "type": "double"
              },
             "created":{
                "type":"date",
                "format" : "epoch_millis"
             }
          }
       }
    }
    return json.dumps(idxjson)