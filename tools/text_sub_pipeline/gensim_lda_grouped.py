
import tools.model.model_classes as merm_model
import tools.text_pipeline.gensim_latent_dirichlet_allocation as gensimlda
import tools.utils.log as log


def analysis_output_label(package:merm_model.PipelinePackage):
    return "Gensim_LDA_by_subset_" + package.any_inputs_dict["previous_task"]
# This class breaks a corpus into subsets and runs LDA on each subset
class GensimLdaGrouped_SubPipe:

    def __init__(self):
        pass

    def _set_analysis(self, package, lda_analysis_by_group):
        analysis_key = analysis_output_label(package)
        if analysis_key in package.any_analysis_dict.keys():
            all_lda = package.any_analysis_dict[analysis_output_label(package)]
            for key, results in lda_analysis_by_group.items():
                all_lda[key] = results
        else:
            package.any_analysis_dict[analysis_key] = lda_analysis_by_group

    def _set_model(self, package:merm_model.PipelinePackage, lda_models_by_group):
        if type(package.model) is dict:
            for key, value in lda_models_by_group.items():
                package.model[key] = value
        else:
            package.model = lda_models_by_group



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
        minimum_doc_count = package.dependencies_dict["env"].config.getint('ml_instructions', 'minimum_doc_count')

        dict_for_group_processing = {}
        dict_for_group_processing["grouped_linked_docs"] = grouped_linked_docs
        dict_for_group_processing["lda_models_by_group"] = lda_models_by_group
        dict_for_group_processing["lda_corpus_by_group"] = lda_corpus_by_group
        dict_for_group_processing["lda_dict_by_group"] = lda_dict_by_group
        dict_for_group_processing["lda_analysis_by_group"] = lda_analysis_by_group
        stop_words = package.dependencies_dict["utils"]._stop_word_list_generator(package)

        for sub_corpus_name , doc_list in grouped_linked_docs.items():
            package_one_group = merm_model.PipelinePackage(lda_models_by_group, lda_corpus_by_group,
                                                     lda_dict_by_group, grouped_linked_docs[sub_corpus_name],
                                                     {}, package.any_inputs_dict,
                                                     package.dependencies_dict)
            package_one_group.any_analysis_dict["stop_words"] = stop_words
            if len(doc_list) >= minimum_doc_count:
                msg = "\n Subset: " + str(sub_corpus_name) + "\n\n"
                log.getLogger().info(msg)
                self._analyze_subset(package_one_group,
                                     dict_for_group_processing,
                                     str(sub_corpus_name),
                                     mfst)

        self._set_analysis(package, lda_analysis_by_group)
        self._set_model(package, lda_models_by_group)

        new_package = merm_model.PipelinePackage(package.model,lda_corpus_by_group,
                                                 lda_dict_by_group,package.linked_document_list,
                                                 package.any_analysis_dict, package.any_inputs_dict,
                                                 package.dependencies_dict)
        return new_package




    def _analyze_subset(self,
                        package_one_group,
                        dict_for_group_processing,
                        sub_corpus_name,
                        manifest):


        package_one_group = manifest["StopWordRemoval"].perform(package_one_group)
        package_one_group = manifest["ListOfListsToGensimCorpora"].perform(package_one_group)
        package_one_group = manifest["GensimLDA"].perform(package_one_group)

        dict_for_group_processing["lda_models_by_group"][sub_corpus_name] = package_one_group.model
        dict_for_group_processing["lda_corpus_by_group"][sub_corpus_name] = package_one_group.corpus
        dict_for_group_processing["lda_dict_by_group"][sub_corpus_name] = package_one_group.dict
        dict_for_group_processing["lda_analysis_by_group"][sub_corpus_name] = package_one_group.any_analysis_dict[gensimlda.lda_analysis_key(package_one_group)]
        overlap_dict = self._topic_overlap(dict_for_group_processing["lda_analysis_by_group"][sub_corpus_name])
        stop_list = self._dynamic_stop_words(overlap_dict, package_one_group.dependencies_dict)
        if len(stop_list) > 4:
            msg = "\n\n=============\nWill try again while removing " + str(stop_list) + " from " + sub_corpus_name
            log.getLogger().info(msg)
            package_one_group.any_analysis_dict = {}
            package_one_group.any_analysis_dict["stop_words"] = stop_list
            package_one_group = self._analyze_subset(package_one_group,
                                                     dict_for_group_processing,
                                                     sub_corpus_name,
                                                     manifest)
        return package_one_group



    def _topic_overlap(self, terms_in_group):

        word_count_dict = {}

        for topic, terms in terms_in_group.items():
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

