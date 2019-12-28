
import tools.model.model_classes as merm_model

import tools.utils.log as log

# This class breaks a corpus into subsets and runs LDA on each subset
class GensimLdaGrouped_SubPipe:

    def __init__(self):
        pass

    def perform(self, package:merm_model.PipelinePackage):
        mfst = package.dependencies_dict["factory"].PipelineManifest.manifest

        #breaks corpus into subsets
        grouped_doc_package = mfst["GroupByESIndex"].perform(package)
        if ("ackage" in type(grouped_doc_package).__name__):
            log.getLogger().info("STRUCTURE after GroupByESIndex:"  + grouped_doc_package.structure())
        else:
            log.getLogger().warning("The return type is not of type PipelinePackage. THIS IS BAD PRACTICE :(")

        grouped_linked_docs = grouped_doc_package.linked_document_list

        lda_models_by_group = {}
        lda_corpus_by_group = {}
        lda_dict_by_group = {}
        lda_analysis_by_group = {}

        dict_for_group_processing = {}
        dict_for_group_processing["grouped_linked_docs"] = grouped_linked_docs
        dict_for_group_processing["lda_models_by_group"] = lda_models_by_group
        dict_for_group_processing["lda_corpus_by_group"] = lda_corpus_by_group
        dict_for_group_processing["lda_dict_by_group"] = lda_dict_by_group
        dict_for_group_processing["lda_analysis_by_group"] = lda_analysis_by_group

        for sub_corpus_name , doc_list in grouped_linked_docs.items():
            if len(doc_list) > 100:
               self._analyze_subset(grouped_doc_package,dict_for_group_processing,grouped_doc_package.any_analysis_dict,sub_corpus_name,mfst,doc_list)

        package.any_analysis_dict[package.default_analysis_key()] = lda_analysis_by_group
        new_package = merm_model.PipelinePackage(lda_models_by_group,lda_corpus_by_group,lda_dict_by_group,grouped_linked_docs,package.any_analysis_dict, package.dependencies_dict)
        return new_package


    def _analyze_subset(self,
                        grouped_doc_package,
                        dict_for_group_processing,
                        any_analysis_dict,
                        sub_corpus_name,
                        manifest,
                        doc_list):
        package_one_group = merm_model.PipelinePackage(grouped_doc_package.model, grouped_doc_package.corpus,
                                                         grouped_doc_package.dict, doc_list,
                                                         any_analysis_dict, grouped_doc_package.any_inputs_dict,
                                                         grouped_doc_package.dependencies_dict)

        package_one_group = manifest["StopWordRemoval"].perform(package_one_group)
        package_one_group = manifest["ListOfListsToGensimCorpora"].perform(package_one_group)
        package_one_group = manifest["GensimLDA"].perform(package_one_group)

        dict_for_group_processing["lda_models_by_group"][sub_corpus_name] = package_one_group.model
        dict_for_group_processing["lda_corpus_by_group"][sub_corpus_name] = package_one_group.corpus
        dict_for_group_processing["lda_dict_by_group"][sub_corpus_name] = package_one_group.dict
        dict_for_group_processing["lda_analysis_by_group"][sub_corpus_name] = package_one_group.any_analysis_dict
        overlap_dict = self._topic_overlap(dict_for_group_processing["lda_analysis_by_group"][sub_corpus_name])
        stop_list = self._dynamic_stop_words(overlap_dict, grouped_doc_package.dependencies_dict)
        if len(stop_list) > 4:
            msg = "\n\n=============\nWill try again while removing " + str(stop_list) + " from " + sub_corpus_name
            log.getLogger().info(msg)
            any_analysis_dict["stop_words"] = stop_list
            package_one_group = self._analyze_subset(grouped_doc_package,dict_for_group_processing,any_analysis_dict,sub_corpus_name,manifest,doc_list)
        return package_one_group



    def _topic_overlap(self, lda_analysis_by_group):

        word_count_dict = {}

        for space, terms in lda_analysis_by_group["default_analysis_key"].items():
            words, weights = zip(*terms)
            for word in words:
                if word in word_count_dict:
                    word_count_dict[word] += 1
                else:
                    word_count_dict[word] = 1
        return word_count_dict


    def _dynamic_stop_words(self,word_count_dict,dependencies_dict):
        permitted_overlap = dependencies_dict["env"].config.getint('ml_instructions', 'gensim_lda_permitted_term_overlap_across_topics')
        stop_list = []
        for word, count in word_count_dict.items():
            if count > permitted_overlap:
                stop_list.append(word)
        return stop_list

