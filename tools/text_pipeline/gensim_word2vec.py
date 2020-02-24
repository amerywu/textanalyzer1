import gensim
import tools.model.model_classes as data_models

import tools.utils.log as log
import gensim

import tools.model.model_classes as data_models
import tools.utils.log as log


class GensimWord2Vec:

    def __init__(self):
        pass

    def perform(self, package:data_models.PipelinePackage):
        doc_list = []
        term_dict = {}
        for linked_doc in package.linked_document_list:
            doc_list.append(linked_doc.tokens)
            term_dict[linked_doc.any_inputs["terms"]] = 1

        model = gensim.models.Word2Vec(
            doc_list,
            size=100,
            window=10,
            min_count=2,
            workers=5,
            iter=10)


        for terms in list(term_dict.keys()):
            term_list = terms.split(" ")
            for term in term_list:
                if term in list(model.wv.index2entity):

                    result = model.wv.most_similar(positive=term)
                    output = "\n_____ " + term + " _____\n"
                    for rel in result:
                        output = output + rel[0] + "\t" + str(rel[1]) + "\n"

                    output = output + "\n - - -\n"
                    log.getLogger().info(output)
        return package

