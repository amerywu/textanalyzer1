from elasticsearch import Elasticsearch

import tools.utils.envutils as env
import tools.utils.log as log
import time

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


def delete_index(index_name):
    try:
        es = connectToES()
        es.indices.delete(index=index_name, ignore=[400, 404])
    except Exception as e:
        msg = "WARN: " +  str(e)
        log.getLogger().error(msg)


def create_and_register_index(index_name:str, body_json):
    try:
        es = connectToES()
        es.indices.create(index=index_name, body=body_json)
    except Exception as e:
        s = str(e)
        log.getLogger().error("Could not create index. " + s)


def retrieve_index_registry():
    es = connectToES()
    results = es.indices.get('*')
    indices = list(results.keys())
    return indices


