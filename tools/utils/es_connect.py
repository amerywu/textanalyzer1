from elasticsearch import Elasticsearch

import tools.utils.envutils as env
import tools.utils.log as log


def connectToES():

    url = env.config["conn_elasticsearch"]["url"]
    port= env.config["conn_elasticsearch"]["port"]
    auth= env.config["conn_elasticsearch"]["auth"]
    scheme = env.config["conn_elasticsearch"]["scheme"]

    if(auth.lower() == "true"):
        es = Elasticsearch(
            [url],
            http_auth=(env.config_a["conn_tx"]["ming"], env.config_a["conn_tx"]["mima"]),
            scheme=scheme,
            port=port,
        )
        return es
    else:
        es = Elasticsearch(
            [url],
            #http_auth=('user', 'secret'),
            scheme=scheme,
            port=port,
        )

        return es



def create_and_register_index(index_name:str, body_json):
    try:

        es = connectToES()





        es.indices.create(index=index_name, ignore=400, body=body_json)
        provider_index_tuple = index_name.split("$$")
        registry_json = { "id" : index_name,
                          "indexname" : index_name,
                          "provider" : provider_index_tuple[0] + "$$"
                          }
        es.index(index="merm_meta$$indexregistry", doc_type='_doc', id=index_name, body= registry_json)
    except Exception as e:
        s = str(e)
        log.getLogger().error("Could not create index. " + s)


def retrieve_index_registry():
    es = connectToES()
    results = es.indices.get('*')
    indices = results.keys()
    for key in indices:
        log.getLogger().info("%d spaces found" +str(key))
    return indices


