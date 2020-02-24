from typing import List
from typing import Tuple
from typing import Dict

from gensim.corpora import Dictionary


def _typeOrNone(obj):
    if(obj is not None):

        thetype=  type(obj).__name__
        if thetype == "list":
            thetype += "["
            for e in obj:
                thetype += _typeOrNone(e)
                break
            thetype += "]"
        elif thetype == "dict":
            if str(thetype) == "dict":
                for k, v in obj.items():
                    thetype = "{"+str(_typeOrNone(k)) + " | " + str(_typeOrNone(v))+"}"
        return thetype

    else:
        return "None"




def category_group_tuple(provider):
    try:
        if "job" in provider:
            return ("majorFinal", "majorFinal")
        elif "corpus_text_rank" in provider:
            return("category", "rank")
        elif "corpus_lda" in provider:
            return("analysis_type", "category")
        elif "corpus_noun_phrase" in provider:
            return("analysis_type", "no_grouping")
        elif "corpus_rake" in provider:
            return("majorFinal", "rank")
        elif "corpus_kmeans" in provider:
            return("cluster", "major")
    except:
        raise ValueError("category_group_tuple unknown provider " + provider)


def linked_document_from_dict(dict, provider):

    if "job" in provider:

        return LinkedDocument(dict["skills"], #raw
                              dict["jobFinal"], #title
                              [], #tokens
                              dict["indexname"], # src
                              None, #ui
                              provider, # provider
                              dict["id"], # uid
                              dict["indexname"], #index_name
                              dict["majorFinal"], # space
                              None, # scores
                              [], # corpus_doc
                              {}, # any_analysis
                              {}, # any_inputs
                              0,
                              dict["groupedBy"] # groupedby
                              )

    elif "corpus_text_rank" in provider:
        return LinkedDocument(dict["sentence"],  # raw
                              dict["category"],  # title
                              [],  # tokens
                              dict["src"],  # src
                              None,  # ui
                              provider,  # provider
                              dict["id"],  # uid
                              dict["src"],  # index_name
                              dict["category"],  # space
                              None,  # scores
                              [],  # corpus_doc
                              {},  # any_analysis
                              {"rank" : dict["rank"]},  # any_inputs
                              dict["created"], # created
                              dict["docid"]  # groupedby
                              )

    elif "corpus_lda" in provider:
        return LinkedDocument(dict["sentence"],  # raw
                              dict["category"],  # title
                              [],  # tokens
                              dict["src"],  # src
                              None,  # ui
                              provider,  # provider
                              dict["id"],  # uid
                              dict["src"],  # index_name
                              dict["analysis_type"],  # space
                              None,  # scores
                              [],  # corpus_doc
                              {},  # any_analysis
                              {},  # any_inputs
                              dict["created"], # created
                              dict["category"]  # groupedby
                              )
    elif "corpus_rake" in provider:
        return LinkedDocument(dict["sentence"],  # raw
                              dict["category"],  # title
                              [],  # tokens
                              dict["src"],  # src
                              None,  # ui
                              provider,  # provider
                              dict["id"],  # uid
                              dict["src"],  # index_name
                              dict["category"],  # space
                              dict["score"],  # scores
                              [],  # corpus_doc
                              {},  # any_analysis
                              {},  # any_inputs
                              dict["created"], # created
                              dict["rank"]  # groupedby
                              )
    elif "corpus_noun_phrase" in provider:
        return LinkedDocument(dict["sentence"],  # raw
                              dict["category"],  # title
                              [],  # tokens
                              dict["src"],  # src
                              None,  # ui
                              provider,  # provider
                              dict["id"],  # uid
                              dict["src"],  # index_name
                              dict["category"],  # space
                              None,  # scores
                              [],  # corpus_doc
                              {},  # any_analysis
                              {},  # any_inputs
                              dict["created"], # created
                              "no_groupings"  # groupedby
                              )
    elif "corpus_kmeans" in provider:
        return LinkedDocument(dict["sentence"],  # raw
                              dict["category"],  # title
                              [],  # tokens
                              dict["src"],  # src
                              None,  # ui
                              provider,  # provider
                              dict["id"],  # uid
                              dict["src"],  # index_name
                              dict["group"],  # space
                              None,  # scores
                              [],  # corpus_doc
                              {},  # any_analysis
                              {"terms" : dict["terms"]},  # any_inputs
                              dict["created"], # created
                              dict["category"]  # groupedby
                              )
    else:
        raise Exception("Unknown provider " + provider)



class LinkedDocument:
    def __init__(self,
                 raw: str,
                 title: str,
                 tokens: List[str],
                 src: str,
                 ui: str,
                 provider: str,
                 uid: str,
                 index_name: str,
                 space: str,
                 scores: dict,
                 corpus_doc: List[Tuple[int, float]],
                 any_analysis: dict,
                 any_inputs: dict,
                 updated: int,
                 groupedBy: str):

        self.raw = raw
        self.title = title
        self.tokens = tokens
        self.source = src
        self.ui = ui
        self.provider = provider
        self.any_analysis = any_analysis
        self.uid = uid
        self.index_name = index_name
        self.corpus_doc = corpus_doc
        self.space = space
        self.scores = scores
        self.updated = updated
        self.groupedBy = groupedBy
        self.any_inputs = any_inputs

    def toString(self):
        return self.ui + " | " + self.title

    def set_analysis(self, analysis):
        self.any_analysis = analysis



    def structure(self):
        return _typeOrNone(self.raw) + "  |  " \
               + _typeOrNone(self.title) + "  |  " \
               + _typeOrNone(self.tokens) + "  |  " \
               + _typeOrNone(self.src) + "  |  " \
               + _typeOrNone(self.ui) + "  |  " \
               + _typeOrNone(self.provider) + "  |  " \
               + _typeOrNone(self.any_analysis) + "  |  " \
               + _typeOrNone(self.uid) + "  |  " \
               + _typeOrNone(self.index_name) + "  |  " \
               + _typeOrNone(self.scores) + "  |  " \
               + _typeOrNone(self.space) + "  |  " \
               + _typeOrNone(self.corpus_doc) + "  |  "




class PipelinePackage:


    def __init__(self,
                 model,
                 corpus,
                 dict,
                 linked_document_list: List[LinkedDocument],
                 any_analysis_dict:  dict,
                 any_inputs_dict: dict,
                 dependencies_dict: dict):
        self.model = model
        self.corpus = corpus
        self.dict = dict
        self.linked_document_list = linked_document_list
        self.any_analysis_dict = any_analysis_dict
        self.any_inputs_dict = any_inputs_dict
        self.dependencies_dict = dependencies_dict
        self.cache_dict = {}


        analysis_type = _typeOrNone(self.any_analysis_dict)
        if analysis_type == "None":
            self.any_analysis_dict = None
        if "{" not in  analysis_type:
            self.any_analysis_dict = { self.default_analysis_key() : any_analysis_dict}
        if type(self.any_inputs_dict) is None:
            self.any_inputs_dict = {}
        if "history" not in self.any_inputs_dict.keys():
            self.any_inputs_dict["history"] = []


    def cache_linked_docs(self):
        self.cache_dict["linked_docs"] = self.linked_document_list.copy()
        self.linked_document_list = []

    def cache_copy_linked_docs(self):
        self.cache_dict["linked_docs"] = self.linked_document_list.copy()

    def uncache_linked_docs(self):
        if "linked_docs" in self.cache_dict.keys() and len(self.cache_dict["linked_docs"]) > 0:
            self.linked_document_list = self.cache_dict["linked_docs"]
            self.cache_dict["linked_docs"] = []

    def log_stage(self, log):
        self.dependencies_dict["log"].getLogger().info(log)
        self.any_analysis_dict["stage_log"] = log

    def stage_log(self):
        return self.any_analysis_dict["stage_log"]

    def default_analysis_key(self):
        return "default_analysis_key"

    def any_analysis(self):
        return self.any_analysis_dict

    def structure(self):
        return "Model: "+ _typeOrNone(self.model) + "  | Corpus: " + _typeOrNone(self.corpus)  + "  | Dictionary:  " + _typeOrNone(self.dict) + "  | Documents: " + _typeOrNone(self.linked_document_list) + " Results/Analysis: " + _typeOrNone(self.any_analysis_dict)


