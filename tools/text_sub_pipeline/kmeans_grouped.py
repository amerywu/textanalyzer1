
import tools.model.model_classes as merm_model

import tools.utils.log as log

# This class breaks a corpus into subsets and runs LDA on each subset
class KmeansGrouped_SubPipe:

    def __init__(self):
        pass


    def perform(self, package:merm_model.PipelinePackage):
        mfst = package.dependencies_dict["factory"].PipelineManifest.manifest

        #breaks corpus into subsets
        grouped_doc_package = mfst["SubsetData"].perform(package)
        if ("ackage" in type(grouped_doc_package).__name__):
            log.getLogger().info("STRUCTURE after SubsetData:"  + grouped_doc_package.structure())
        else:
            log.getLogger().warning("The return type is not of type PipelinePackage. THIS IS BAD PRACTICE :(")

        grouped_linked_docs = grouped_doc_package.linked_document_list
        analysis_by_group_kmeans = {}

        minimum_doc_count = package.dependencies_dict["env"].config.getint('ml_instructions', 'minimum_doc_count')
        log_string = "\n======================\nSubset Analysis for text rank, rake and noun phrase.\n"
        for sub_corpus_name_untyped , doc_list in grouped_linked_docs.items():
            sub_corpus_name = str(sub_corpus_name_untyped)
            if len(doc_list) > minimum_doc_count:
                package_one_group = merm_model.PipelinePackage(package.model,
                                                               package.corpus,
                                                               package.dict,
                                                               doc_list,
                                                               {},
                                                               package.any_inputs_dict,
                                                               package.dependencies_dict)
                package_one_group.any_analysis_dict["provider"] = package.any_analysis_dict["provider"]
                package_one_group.any_inputs_dict["corpus_name"] = sub_corpus_name
                package_one_group = self._analyze_subset(package_one_group,sub_corpus_name,mfst,doc_list)
                for key, results in package_one_group.any_analysis_dict.items():
                    if key.startswith("km"):
                        package.any_analysis_dict[key] = results



                log_string = log_string + package_one_group.stage_log()

        package.any_analysis_dict["noun_phrase_all_groups"] = analysis_by_group_kmeans
        new_package = merm_model.PipelinePackage(package.model,
                                                 package.corpus,
                                                 package.dict,
                                                 grouped_linked_docs,
                                                 package.any_analysis_dict,
                                                 package.any_inputs_dict,
                                                 package.dependencies_dict)

        new_package.log_stage(log_string)
        return new_package


    def _analyze_subset(self,
                        package_one_group,
                        sub_corpus_name,
                        manifest,
                        doc_list):

        log.getLogger().info("Subset: " + str(sub_corpus_name))
        log.getLogger().info(":TextRank: ")
        log_string = "\n\n\n++++++++++++++++++++++++++++++\nSubset: " + str(sub_corpus_name)




        package_one_group.cache_copy_linked_docs()

        package_one_group = manifest["LinkedDocCorpusWordCount"].perform(package_one_group)
        log_string = log_string + package_one_group.stage_log()

        package_one_group = manifest["LinkedDocCorpusStopWordGenerator"].perform(package_one_group)
        log_string = log_string + package_one_group.stage_log()

        package_one_group = manifest["StopWordRemoval"].perform(package_one_group)
        log_string = log_string + package_one_group.stage_log()


        package_one_group = manifest["FilterTokensByCount"].perform(package_one_group)
        log_string = log_string + package_one_group.stage_log()

        package_one_group = manifest["LinkedDocCorpusWordCount"].perform(package_one_group)
        log_string = log_string + package_one_group.stage_log()

        package_one_group = manifest["TokensToDoc"].perform(package_one_group)
        log_string = log_string + package_one_group.stage_log()

        package_one_group = manifest["LinkedDocListToScikitRFCorpus"].perform(package_one_group)
        log_string = log_string + "\n\n" + package_one_group.stage_log()

        log.getLogger().info(":ScikitKmeansWithTermRemoval: ")
        package_one_group = manifest["ScikitKmeansWithTermRemoval"].perform(package_one_group)
        log_string = log_string + "\n\n" + package_one_group.stage_log()

        package_one_group.uncache_linked_docs()


        package_one_group = manifest["KmeansSentenceFinder"].perform(package_one_group)
        log_string = log_string + "\n\n" + package_one_group.stage_log()




        package_one_group.log_stage(log_string)

        return package_one_group



