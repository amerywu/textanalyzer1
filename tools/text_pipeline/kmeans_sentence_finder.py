from typing import List
from typing import Tuple
import tools.model.model_classes as merm_model
import tools.utils.text_parsing_utils as parser
import re
import tools.utils.log as log
import time
import uuid
import json

class KmeansSentenceFinder:

    def __init__(self):
        pass

    def _prep_es(self, package, toes):
        if toes == True:
            ingestor = package.dependencies_dict["ingestor"]
            rebuild_idx = package.dependencies_dict["env"].config.getboolean("job_instructions","es_recreate_index")
            if rebuild_idx:
                ingestor.delete_index("corpus_kmeans")
            ingestor.create_index(self._newindex_json(), "corpus_kmeans")

    def perform(self, package: merm_model.PipelinePackage):
        env = package.dependencies_dict["env"]
        toes = env.config.getboolean("job_instructions", "output_to_elasticsearch")
        index_suffix = env.config["job_instructions"] ["kmeans_index_suffix"]
        self._prep_es(package, toes)
        csv_list_of_lists = []
        csv_list_of_lists.append(["text_source", "major", "topic_id", "term", "weight"])
        kmeans_top_terms_key = package.any_inputs_dict["kmeans_top_terms_key"]
        kmeans_top_terms = package.any_analysis_dict[kmeans_top_terms_key]
        kmeans_top_terms.pop(0)

        term_by_cluster_dict = {}
        for row in kmeans_top_terms:
            cluster = row.pop(0)
            term_by_cluster_dict[cluster] = row

        cluster_list = []
        for cluster_id, terms_List in term_by_cluster_dict.items():
            salient_sentences = self._find_salient_sentences(terms_List, package, cluster_id)
            cluster_list.append(salient_sentences)

            log.getLogger().info("Kmeans Sentences saved " + str(len(salient_sentences)) + " rows")
            if True == toes:
                self.__dispatch_to_elastic_search(salient_sentences, package.any_analysis_dict["provider"], package.dependencies_dict["ingestor"], index_suffix)

        package.log_stage("Identified salient sentences for each cluster")
        package.any_analysis_dict[kmeans_top_terms_key + "_sentences"] = cluster_list

        return package

    def _list_to_string(self, alist):
        if len(alist) == 0:
            return ""
        elif type(alist) is str:
            return alist
        else:
            astring = ""
            for token in alist:
                astring = astring + " " + str(token)
            return astring

    def _recompose_to_list_of_strings(self, corpus_dict):
        list_of_lists = []
        for key, alist in corpus_dict.items():
            if len(alist) > 0 and type(alist[0]) is list:
                for inner_list in alist:
                    list_of_lists.append(key + ",", self._list_to_string(alist) + ", " + self._list_to_string(inner_list))
            else:
                for e in alist:
                    list_of_lists.append(self._list_to_string(key) + "," + e)
        return list_of_lists

    def _break_corpus_as_sentences(self, docs_list: List[merm_model.LinkedDocument]):
        corpus_as_sentences = []
        for doc in docs_list:
            corpus_as_sentences = corpus_as_sentences + parser.split_linked_doc_by_sentence(doc)
        corpus_as_sentences = parser.tokenize(corpus_as_sentences)
        corpus_as_sentences = parser.lemmatize_tokens(corpus_as_sentences, parser.standard_stop_words())
        return corpus_as_sentences

    def _find_salient_sentences(self, top_terms_list, package, clusterid):
        all_sentences = self._get_sentences(package)
        returned_values =  self._compile_sentences(all_sentences, top_terms_list, clusterid)
        return returned_values

    def _compile_sentences(self, sentences, topiclist_in, clusterid):
        return_dict = {}
        topiclist = topiclist_in.copy()

        term_groups = []

        while (len(topiclist) > 1):
            anchor = topiclist.pop(0)

            for i in range (len(topiclist)) :
                if len(topiclist) > i + 1:
                    words3 = [anchor, topiclist[i], topiclist[i+1]]
                    term_groups.append(words3)
                    log.getLogger().info("words3 " + str(len(words3)) + "term_groups " + str(len(term_groups)))

                words2 = [anchor, topiclist[i]]
                term_groups.append(words2)
                log.getLogger().info("words2 " + str(len(words2)) + str(len(term_groups)))


        salient_sentences1 = self._words_in_sentence_list2(term_groups, sentences, clusterid )
        log.getLogger().info("Found " + str(len(salient_sentences1)) + " sentences\n\n\n")
        return salient_sentences1




    def _words_in_sentence_list2(self, term_groups: List[str], sentences, clusterid):
        raw_sentences = []
        for sentence in sentences:
            #log.getLogger().info(sentence)
            for terms in term_groups:
                sentence_words = sentence[2].lower().split(" ")

                if self._tokens_match(terms, sentence_words) == True:
                    #log.getLogger().info("***Match " + str(terms))
                    row_list = []
                    row_list.append(self._list_to_string(terms))
                    row_list.append(clusterid)
                    row_list.append(sentence[0])
                    row_list.append(sentence[1])
                    row_list.append(sentence[2])
                    row_list.append(sentence[3])
                    raw_sentences.append(row_list)
                    log.getLogger().info("added 1 for terms " + str(terms))
        return raw_sentences

    def _tokens_match(self, terms, sentence_words):
        for token in terms:
            if not token in sentence_words:
                return False
        return True

    def _is_by_rank(self, topicid):
        thetype = type(topicid)
        if "int" in type(topicid).__name__ :
            return False
        else:
            regexp = re.compile(r'_[0-9]')
            return regexp.search(topicid)

    def _get_sentences(self, package: merm_model.PipelinePackage):

        sentences = []
        for linked_doc in package.linked_document_list:
            sentences.append((linked_doc.space, linked_doc.groupedBy, linked_doc.raw, linked_doc.uid))
        return sentences

    def  _get_text_rank_sentences_list_based(self, package: merm_model.PipelinePackage, topicid):
        all_sentences = []
        for linked_doc in package.linked_document_list:
            all_sentences.append(linked_doc.raw)
        return all_sentences

    def _id_generator(self):
        return str(uuid.uuid1())

    def _index_dict(self, index_name, src, terms, category, group, sentence, doc_id):
        adict = {
            '_index': index_name,
            '_id': self._id_generator(),
            '_source': self._generate_doc(src, terms, category, group, sentence, doc_id)
        }
        return adict

    def _generate_doc(self,  src, terms, category, group, sentence, doc_id):
        data = {}
        data["doc_id"] = doc_id
        data['category'] = category
        data['group'] = group
        data['terms'] = terms
        data['sentence'] = sentence
        data['src'] = src
        data["created"] = int(round(time.time() * 1000))
        return data

    def _id_generator(self):
        return str(uuid.uuid1())

    def __dispatch_to_elastic_search(self, sentences_by_terms, src, ingestor, index_suffix):
        count = 0
        bulk_list = []

        for sentence_data in sentences_by_terms:

            count = count + 1
            if count % 5000 == 0:
                log.getLogger().info("dispatching" + str(count))
                ingestor._dispatch_bulk("corpus_kmeans" + index_suffix, bulk_list)
                bulk_list = []
            bulk_list.append(self._index_dict("corpus_kmeans" + index_suffix, src, sentence_data[0], sentence_data[1], sentence_data[2], sentence_data[3], sentence_data[4]))
                        # ingestor._dispatch("corpus_text_rank", _id_generator(),json)

        ingestor._dispatch_bulk("corpus_kmeans" + index_suffix, bulk_list)
        log.getLogger().info("Dispatched " + str(count) + " rows to ES")

    # "format": "yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||epoch_millis"
    def _newindex_json(self):
        idxjson = {
            "settings": {
                "number_of_shards": 3,
                "number_of_replicas": 2
            },
            "mappings": {
                "properties": {
                    "id": {
                        "type": "text"
                    },
                    "src": {
                        "type": "text"
                    },
                    "category": {
                        "type": "keyword"
                    },
                    "docid": {
                        "type": "text"
                    },
                    "group": {
                        "type": "keyword"
                    },
                    "terms": {
                        "type": "keyword"
                    },
                    "sentence": {
                        "type": "text"
                    },
                    "created": {
                        "type": "date",
                        "format": "epoch_millis"
                    }
                }
            }
        }
        return json.dumps(idxjson)

