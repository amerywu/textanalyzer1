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
        thetype = type(package.linked_document_list)
        if thetype  is dict:
            return package

        linked_doc_by_index = {}
        group_testenv = package.dependencies_dict["env"].config.getboolean("pipeline_instructions","group_testenv")
        if True == group_testenv:
            relevant_groups_string = package.dependencies_dict["env"].config["pipeline_instructions"] ["group_testenv_list"]
            relevant_groups_list = relevant_groups_string.split(",")
            linked_doc_by_index = self._group_test_groups(package,relevant_groups_list)
        else:
            linked_doc_by_index = self._group_all_groups(package)


        new_package = merm_model.PipelinePackage(package.model, package.corpus, package.dict, linked_doc_by_index,
                                                 package.any_analysis, package.any_inputs_dict, package.dependencies_dict)
        return new_package


    def _group_all_groups(self, package):
        linked_doc_by_index = {}
        for linked_doc in package.linked_document_list:

            if linked_doc.groupedBy in linked_doc_by_index:
                linked_doc_by_index[linked_doc.groupedBy].append(linked_doc)
            else:
                groupby_list = []
                groupby_list.append(linked_doc)
                linked_doc_by_index[linked_doc.groupedBy] = groupby_list

        return linked_doc_by_index

    def _group_test_groups(self, package, relevant_groups_list):
        linked_doc_by_index = {}
        for linked_doc in package.linked_document_list:

            if linked_doc.groupedBy in relevant_groups_list:

                if linked_doc.groupedBy in linked_doc_by_index:
                    linked_doc_by_index[linked_doc.groupedBy].append(linked_doc)
                else:
                    groupby_list = []
                    groupby_list.append(linked_doc)
                    linked_doc_by_index[linked_doc.groupedBy] = groupby_list

        return linked_doc_by_index


class StopWordRemoval:

    def __init__(self):
        self.stop_words_key = "stop_words"
        pass

    def perform(self, package: merm_model.PipelinePackage):


        log.getLogger().info("StopWordRemoval")
        stop_words = []
        if self.stop_words_key in package.any_analysis_dict:
            log.getLogger().info("Stop words retrieved from package analyses.")
            log.getLogger().debug("It's a list")
            stop_words = package.any_analysis_dict[self.stop_words_key]

        else:

            stop_list_path = package.dependencies_dict["env"].config["job_instructions"]["stop_list"]
            stop_list_string = package.dependencies_dict["env"].read_file(stop_list_path)
            log.getLogger().info("Stop words from file system")

            if package.dependencies_dict["utils"]._charFrequency(stop_list_string, ",") > 5:
                log.getLogger().info("comma delineated")
                stop_words = stop_list_string.split(",")
            else:
                log.getLogger().info("\\n delineated")
                stop_words = stop_list_string.split("\n")

        for linked_doc in package.linked_document_list:
            new_tokens = []
            for word in linked_doc.tokens:
                if not word in stop_words:
                    # log.getLogger().debug("removing " + word)
                    new_tokens.append(word)
            linked_doc.tokens = new_tokens

        return package


