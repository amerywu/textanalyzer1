
import tools.model.model_classes as merm_model

import tools.utils.log as log

# This class breaks a corpus into subsets and runs LDA on each subset
class StopWord_SubPipe:

    def __init__(self):
        pass


    def perform(self, package:merm_model.PipelinePackage):
        mfst = package.dependencies_dict["factory"].PipelineManifest.manifest

        #breaks corpus into subsets
        grouped_doc_package = mfst["SubsetData"].perform(package)


        stop_word_applied_linked_docs = []
        grouped_linked_docs = grouped_doc_package.linked_document_list
        log_string = "\n======================\nSubset Stopword removal.\n"
        for sub_corpus_name_untyped , doc_list in grouped_linked_docs.items():
            sub_corpus_name = str(sub_corpus_name_untyped)

            package_one_group :merm_model.PipelinePackage = merm_model.PipelinePackage(package.model,
                                                               package.corpus,
                                                               package.dict,
                                                               doc_list,
                                                               {},
                                                               package.any_inputs_dict,
                                                               package.dependencies_dict)
            package_one_group.any_inputs_dict["corpus_name"] = sub_corpus_name
            package_one_group = self._analyze_subset(package_one_group,sub_corpus_name,mfst,doc_list)
            stop_word_applied_linked_docs = stop_word_applied_linked_docs + package_one_group.linked_document_list
            log_string = log_string + package_one_group.stage_log()


        new_package = merm_model.PipelinePackage(package.model,
                                                 package.corpus,
                                                 package.dict,
                                                 stop_word_applied_linked_docs,
                                                 package.any_analysis_dict,
                                                 package.any_inputs_dict,
                                                 package.dependencies_dict)

        new_package.log_stage(log_string)

        if ("ackage" in type(new_package).__name__):
            log.getLogger().info("STRUCTURE after SubsetData:"  + new_package.structure())
        else:
            log.getLogger().warning("The return type is not of type PipelinePackage. THIS IS BAD PRACTICE :(")

        return new_package


    def _analyze_subset(self,
                        package_one_group,
                        sub_corpus_name,
                        manifest,
                        doc_list):

        log.getLogger().info("Subset: " + str(sub_corpus_name))
        log.getLogger().info(":TextRank: ")
        log_string = "\n\n\n++++++++++++++++++++++++++++++\nSubset: " + str(sub_corpus_name)

        package_one_group = manifest["LinkedDocCorpusWordCount"].perform(package_one_group)
        log_string = log_string + package_one_group.stage_log()

        log.getLogger().info(":LinkedDocCorpusStopWordGenerator: ")
        package_one_group = manifest["LinkedDocCorpusStopWordGenerator"].perform(package_one_group)
        log_string = log_string + "\n\n" + package_one_group.stage_log()

        package_one_group = manifest["StopWordRemoval"].perform(package_one_group)
        log_string = log_string + package_one_group.stage_log()

        package_one_group = manifest["LinkedDocCorpusWordCount"].perform(package_one_group)
        log_string = log_string + package_one_group.stage_log()

        log_string = log_string + "\n\n" + package_one_group.stage_log()
        package_one_group.log_stage(log_string)

        return package_one_group



