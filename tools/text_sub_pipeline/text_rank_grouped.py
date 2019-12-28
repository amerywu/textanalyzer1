
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
        analysis_by_group_rake = {}
        analysis_by_group_text_rank = {}

        for sub_corpus_name_untyped , doc_list in grouped_linked_docs.items():
            sub_corpus_name = str(sub_corpus_name_untyped)
            if len(doc_list) > 100:

                package_one_group = merm_model.PipelinePackage(package.model,
                                                               package.corpus,
                                                               package.dict,
                                                               doc_list,
                                                               {},
                                                               package.any_inputs_dict,
                                                               package.dependencies_dict)
                package_one_group.any_inputs_dict["corpus_name"] = sub_corpus_name
                package_one_group = self._analyze_subset(package_one_group,sub_corpus_name,mfst,doc_list)
                analysis_by_group_text_rank[sub_corpus_name] = package_one_group.any_analysis_dict["text_rank_0"]
                analysis_by_group_rake[sub_corpus_name] = package_one_group.any_analysis_dict["rake_0"]
                analysis_by_group_text_rank[sub_corpus_name + "_lemmatized"] = package_one_group.any_analysis_dict["text_rank_1"]
                analysis_by_group_rake[sub_corpus_name + "_lemmatized"] = package_one_group.any_analysis_dict["rake_1"]
        package.any_analysis_dict["text_rank_all_groups"] = analysis_by_group_text_rank
        package.any_analysis_dict["rake_all_groups"] = analysis_by_group_rake

        new_package = merm_model.PipelinePackage(package.model,
                                                 package.corpus,
                                                 package.dict,
                                                 grouped_linked_docs,
                                                 package.any_analysis_dict,
                                                 package.any_inputs_dict,
                                                 package.dependencies_dict)
        return new_package


    def _analyze_subset(self,
                        package_one_group,
                        sub_corpus_name,
                        manifest,
                        doc_list):


        log.getLogger().info("Subset: " + str(sub_corpus_name))
        log.getLogger().info(":TextRank: ")
        package_one_group = manifest["TextRank"].perform(package_one_group)

        log.getLogger().info(":LinkedDocCorpusStopWordGenerator: ")
        package_one_group = manifest["LinkedDocCorpusStopWordGenerator"].perform(package_one_group)

        log.getLogger().info(":RakeAnalysisFromTextRank: ")
        package_one_group = manifest["RakeAnalysisFromTextRank"].perform(package_one_group)


        log.getLogger().info(":Lemmatize_Corpus_LinkedDocs: ")
        package_one_group = manifest["Lemmatize_Corpus_LinkedDocs"].perform(package_one_group)

        log.getLogger().info(":TextRank: ")
        package_one_group = manifest["TextRank"].perform(package_one_group)

        log.getLogger().info(":RakeAnalysisFromTextRank: ")
        package_one_group = manifest["RakeAnalysisFromTextRank"].perform(package_one_group)
        return package_one_group



