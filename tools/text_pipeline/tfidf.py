from operator import itemgetter

import numpy as np
from gensim.models import TfidfModel

import tools.model.model_classes as merm_model
import tools.utils.log as log


class GensimTfIdfModelBuilder:

    def __init__(self):
        pass

    def perform(self, package:merm_model.PipelinePackage):
        log.getLogger().info( "Generating Gensim TF-IDF model")

        model = TfidfModel(package.corpus)  # fit model

        return merm_model.PipelinePackage(model,package.corpus,package.dict,package.linked_document_list,
                                          package.any_analysis_dict, package.any_inputs_dict,
                                          package.dependencies_dict)


class GensimTfIdfTopTerms:

    def __init__(self):
         pass

    def perform(self, package:merm_model.PipelinePackage):
        log.getLogger().info( "Analyzing Gensim TF-IDF model")
        log.getLogger().info("Corpus size: " + str(len(package.linked_document_list)))
        self._validate(package)
        idx = 0
        top_tf_idf_corpus = []

        for model_result in package.model[package.corpus]:
            top_tfidf_doc =[]
            sorteddoc = sorted(model_result, key=itemgetter(1), reverse = True)
            linked_doc_source = package.linked_document_list[idx]
            for id, freq in sorteddoc[:10]:
                top_tfidf_doc.append((package.dict[id], np.around(freq, decimals = 3)))

            str1 = "\n\n\n"
            log.getLogger().debug(str1)
            top_tf_idf_corpus.append(top_tfidf_doc)

            linked_doc_source.any_analysis = top_tfidf_doc
            idx = idx +1

        package.any_analysis_dict[package.default_analysis_key()] = top_tf_idf_corpus

        return merm_model.PipelinePackage(package.model, package.corpus, package.dict,
                                          package.linked_document_list, package.any_analysis_dict,
                                          package.any_inputs_dict, package.dependencies_dict)



    def _validate(self, package:merm_model.PipelinePackage):
        docidx = 0
        for corpus_doc in package.model[package.corpus]:
            linked_doc = package.linked_document_list[docidx]
            for id, freq in corpus_doc:

                if package.dict[id] not in linked_doc.tokens:
                    log.getLogger().error("NOT FOUND " + package.dict[id])
                    raise Exception ("NOT FOUND " + package.dict[id] +". Corpus out of sync with source corpus")

                linked_doc.corpus_doc = sorted(corpus_doc, key=itemgetter(1), reverse=True)
            docidx = docidx + 1




