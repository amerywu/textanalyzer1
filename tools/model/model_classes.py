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




def linked_document_from_dict(dict, provider):

    if "job" in provider:
        return LinkedDocument(dict["skills"],
                              dict["jobFinal"],
                              None,
                              dict["indexname"],
                              None,
                              provider,
                              dict["id"],
                              dict["indexname"],
                              dict["majorFinal"],
                              None,
                              None,
                              None,
                              None,
                              dict["groupedBy"]
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
                 any_analysis,
                 updated: int,
                 groupedBy: str = ""):

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


    def __init__(self, model, corpus, dict, linked_document_list: List[LinkedDocument], any_analysis_dict:Dict, dependencies_dict:Dict):
        self.model = model
        self.corpus = corpus
        self.dict = dict
        self.linked_document_list = linked_document_list
        self.any_analysis_dict = any_analysis_dict
        self.dependencies_dict = dependencies_dict

        analysis_type = _typeOrNone(self.any_analysis_dict)
        if analysis_type == "None":
            self.any_analysis_dict = None
        if "{" not in  analysis_type:
            self.any_analysis_dict = { self.default_analysis_key() : any_analysis_dict}


    def default_analysis_key(self):
        return "default_analysis_key"

    def any_analysis(self):
        return self.any_analysis_dict[self.default_analysis_dic]

    def structure(self):
        return "Model: "+ _typeOrNone(self.model) + "  | Corpus: " + _typeOrNone(self.corpus)  + "  | Dictionary:  " + _typeOrNone(self.dict) + "  | Documents: " + _typeOrNone(self.linked_document_list) + " Results/Analysis: " + _typeOrNone(self.any_analysis_dict)


