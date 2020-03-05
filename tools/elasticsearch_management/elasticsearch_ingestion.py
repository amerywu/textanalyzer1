import json
from typing import Dict
import time
import tools.model.model_classes as merm_model
import tools.elasticsearch_management.es_connect as es_conn
import tools.utils.log as log
from elasticsearch import Elasticsearch, helpers

def delete_index(index_name):
    try:
        es_conn.delete_index(index_name)
        time.sleep(5)
    except Exception as e:
        msg = "WARN: " +  str(e)
        log.getLogger().error(msg)
        pass

def create_index(index_json, index_name):
    index_results = es_conn.retrieve_index_registry()
    if index_name not in index_results:
        es_conn.create_and_register_index(index_name, index_json)


def _dispatch(index_name, id, bodyjson, retry_count=0):
    try:
        es = es_conn.connectToES()
        es.index(index=index_name,  id=id, body=bodyjson)
    except Exception as e:
        retry_count = retry_count + 1
        msg = "WARN: " +  str(e)

        log.getLogger().error(msg)
        if "time" in msg.lower() and retry_count < 10:
            _dispatch(index_name, id, bodyjson, retry_count)
        else:
            pass


def _dispatch_bulk(index_name, data_dict, retry_count=0):
    try:
        es = es_conn.connectToES()
        helpers.bulk(es, data_dict, index=index_name)
    except Exception as e:
        retry_count = retry_count + 1
        msg = "WARN: " +  str(e)

        log.getLogger().error(msg)
        if "time" in msg.lower() and retry_count < 10:
            _dispatch_bulk(index_name,data_dict, retry_count)
        else:
            pass






