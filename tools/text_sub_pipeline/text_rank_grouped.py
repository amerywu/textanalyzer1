
import tools.model.model_classes as merm_model

import tools.utils.log as log

# This class breaks a corpus into subsets and runs LDA on each subset
class TextRankGrouped_SubPipe:

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

        package.any_analysis_dict["text_rank_all_groups"] = lda_analysis_by_group
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
                                                         any_analysis_dict, grouped_doc_package.dependencies_dict)

        log.getLogger().info("Subset: " + sub_corpus_name)
        package_one_group = manifest["TextRank"].perform(package_one_group)


        dict_for_group_processing["lda_models_by_group"][sub_corpus_name] = package_one_group.model
        dict_for_group_processing["lda_corpus_by_group"][sub_corpus_name] = package_one_group.corpus
        dict_for_group_processing["lda_dict_by_group"][sub_corpus_name] = package_one_group.dict
        dict_for_group_processing["lda_analysis_by_group"][sub_corpus_name] = package_one_group.any_analysis_dict

        return package_one_group



