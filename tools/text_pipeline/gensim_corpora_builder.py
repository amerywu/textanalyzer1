from gensim import corpora

import tools.model.model_classes as data_models
import tools.utils.dfutils as dfutils

import tools.utils.log as log



class ListOfListsToGensimCorpora:

    def __init__(self):
        pass

    def perform(self, package:data_models.PipelinePackage):
        linked_doc_list = package.linked_document_list
        log.getLogger().info("Converting corpora as bag of words. Input format is List[List[str]]. Output is Gensim Dictionary")
        log.getLogger().info("Corpus size: " + str(len(package.linked_document_list)))
        bowlist = []
        for doc in linked_doc_list:
            bowlist.append(doc.tokens)

        dictionary = corpora.Dictionary(bowlist)

        #log.getLogger().info(dictionary)
        log.getLogger().info("Incoming doc count: " + str(len(linked_doc_list)))
        corpus = [dictionary.doc2bow(line) for line in bowlist]

        for doc in corpus:
            doc
            for word, freq in doc:
                 res = dictionary.get(word)
                 res
        log.getLogger().info("Feature count: " + str(len(dictionary.id2token)))

        return data_models.PipelinePackage(None, corpus, dictionary, linked_doc_list, package.any_analysis_dict, package.any_inputs_dict, package.dependencies_dict)




class DataframeToListOfLists:

    def __init__(self):
        pass

    def perform(self, package:data_models.PipelinePackage):
        df = package.corpus
        log.getLogger().info("Stage: Converting dataframe of documents (previously mapped through DataFrameConvertForPipeline) to tokenized and lemmatized List[List[str]]. Outer List is corpora, inner list is document as bag of words")
        log.getLogger().info("Corpus size: " + str(df.shape))
        corpora_list = self._dfToList(package)
        tokenized_linked_docs = package.dependencies_dict["utils"].tokenize(corpora_list)
        #merm_tools_linkeddocument_list =package.dependencies_dict["utils"].lemmatize_tokens(token_list, package.dependencies_dict["utils"].standard_stop_words())
        package = data_models.PipelinePackage(None, None, None, tokenized_linked_docs, package.any_analysis_dict, package.any_inputs_dict, package.dependencies_dict)
        return package

    def _dfToList(self, package:data_models.PipelinePackage):
        dict_list = dfutils.dfToList(package.corpus)

        corpora_list = []


        for d in dict_list:
            corpora_list.append(data_models.linked_document_from_dict(d, package.any_analysis_dict["provider"]))
            # log.getLogger().debug("Sample text " + cleaned)
        return corpora_list






