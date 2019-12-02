from gensim import corpora

import tools.model.model_classes as merm_model
import tools.utils.dfutils as dfutils
import tools.utils.envutils as env
import tools.utils.log as log
import tools.utils.text_parsing_utils
import tools.elasticsearch_extraction.es_extraction as esextract
import tools.utils.es_connect as esconn


class GroupByESIndex:

    def __init__(self):
        pass

    def perform(self, package: merm_model.PipelinePackage):

        linked_doc_by_index = {}

        for linked_doc in package.linked_document_list:

            if linked_doc.groupedBy in linked_doc_by_index:
                linked_doc_by_index[linked_doc.groupedBy].append(linked_doc)
            else:
                groupby_list = []
                groupby_list.append(linked_doc)
                linked_doc_by_index[linked_doc.groupedBy] = groupby_list

        new_package = merm_model.PipelinePackage(package.model, package.corpus, package.dict, linked_doc_by_index,
                                                 package.any_analysis, package.dependencies_dict)
        return new_package


class StopWordRemoval:

    def __init__(self):
        self.stop_words_key = "stop_words"
        pass

    def perform(self, package: merm_model.PipelinePackage):

        log.getLogger().info("StopWordRemoval (If stop word list present in package.any_analysis)")
        if self.stop_words_key in package.any_analysis_dict:
            log.getLogger().debug("got stop words")

            log.getLogger().debug("It's a list")
            stop_words = package.any_analysis_dict[self.stop_words_key]
            for linked_doc in package.linked_document_list:
                new_tokens = []
                for word in linked_doc.tokens:
                    if not word in stop_words:
                        # log.getLogger().debug("removing " + word)
                        new_tokens.append(word)
                linked_doc.tokens = new_tokens
        return package
