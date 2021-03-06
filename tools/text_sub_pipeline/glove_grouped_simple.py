
import tools.model.model_classes as merm_model

import tools.utils.log as log

# This class breaks a corpus into subsets and runs LDA on each subset
class GloveGrouped_SubPipe:

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
        analysis_by_group_glove = {}
        analysis_by_group_glove_loadings = {}
        env = package.dependencies_dict["env"]
        dimensions = env.config.getint("ml_instructions", "glove_dimensions")
        minimum_doc_count = env.config.getint('ml_instructions', 'minimum_doc_count')
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
                package_one_group.any_inputs_dict["corpus_name"] = sub_corpus_name
                package_one_group = self._analyze_subset(package_one_group,sub_corpus_name,mfst,doc_list)
                analysis_by_group_glove[sub_corpus_name] = package_one_group.any_analysis_dict[str(dimensions)+"d_glove_output"]
                analysis_by_group_glove_loadings[sub_corpus_name] = package_one_group.any_analysis_dict[str(dimensions) + "d_glove_biggest_loadings"]

                log_string = log_string + package_one_group.stage_log()

        package.any_analysis_dict["glove_all_groups"] = analysis_by_group_glove
        package.any_analysis_dict["glove_all_loadings"] = analysis_by_group_glove_loadings

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

        package_one_group = manifest["LinkedDocCorpusWordCount"].perform(package_one_group)
        log_string = log_string + package_one_group.stage_log()

        package_one_group = manifest["GloveModelBuilder"].perform(package_one_group)
        log_string = log_string + "\n\n" + package_one_group.stage_log()


        package_one_group = manifest["GloveLoadings"].perform(package_one_group)
        log_string = log_string + "\n\n" + package_one_group.stage_log()

        package_one_group.log_stage(log_string)

        return package_one_group



