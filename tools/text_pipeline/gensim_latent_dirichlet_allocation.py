import typing
import pprint
import tools.model.model_classes as merm_model
import tools.utils.log as log
import tools.utils.envutils as env
import gensim
import csv
import pandas as pd

class GensimLDA:

    def __init__(self):
        pass


    def perform(self, package:merm_model.PipelinePackage):
        #scipy_csc_matrix = gensim.matutils.corpus2csc(package.corpus)
        log.getLogger().info("STAGE: Running a standard LDA in Gensim")
        topic_count = env.config.getint('ml_instructions', 'gensim_lda_topics')
        log.getLogger().info("Seeking " + str(topic_count) + " topics")
        report_word_count = env.config.getint('ml_instructions', 'gensim_lda_term_per_topic_reporting_count')
        if len(package.dict.token2id) > 50:
            topic_dict = {}



            lda_model = gensim.models.ldamodel.LdaModel(corpus=package.corpus,
                                                        id2word=package.dict,
                                                        num_topics=topic_count,
                                                        update_every=1,
                                                        alpha='auto',
                                                        per_word_topics=False,
                                                        iterations= 100)


            for index, topic in lda_model.show_topics(formatted=False, num_words=report_word_count):
                #print('Topic: {} \nWords: {}'.format(index, [w[0] for w in topic]))
                words_for_topic = []
                for w in topic:
                    msg = str(index) + ":" + str(w)
                    log.getLogger().info(msg)
                    words_for_topic.append((w[0],w[1]))
                topic_dict[index] = words_for_topic

            package.any_analysis_dict[package.default_analysis_key()] = topic_dict
            new_package = merm_model.PipelinePackage(lda_model,package.corpus,package.dict,package.linked_document_list,package.any_analysis_dict, package.any_inputs_dict, package.dependencies_dict)
            return new_package
        else:
            new_package = merm_model.PipelinePackage(None, package.corpus, package.dict,
                                                       package.linked_document_list, [], package.dependencies_dict)
            return new_package


class GensimTopicSimilarityAnalysis:

    def __init__(self):
        pass


    def perform(self, package:merm_model.PipelinePackage):
        #scipy_csc_matrix = gensim.matutils.corpus2csc(package.corpus)
        log.getLogger().info("STAGE: Seeking to identify similar topics across multiple corpii")
        prepare_data = self._prepare_data(package)
        matching_topics = self._iterate_similar_topics(prepare_data)
        package.any_analysis_dict[package.default_analysis_key()] = matching_topics

        return merm_model.PipelinePackage(package.model,package.corpus,package.dict,
                                          package.linked_document_list,package.any_analysis_dict,
                                          package.any_inputs_dict, package.dependencies_dict)

    def load_top_level_topic_csv(self):
        file = env.config["ml_instructions"]["gensim_top_level_topic_csv"]
        top_level_topics = csv.read(file)
        top_level_topics


    def _prepare_data(self, package:merm_model.PipelinePackage):
        topics_as_terms_by_idx = {}
        for idxname, topicdict in package.any_analysis().items():
            topics_as_terms = {}
            msg = "\n\n\n+++++++++++++++++++\n\nSimilarity Search " + idxname + "\n\n"
            log.getLogger().debug(msg)
            docs_list = package.linked_document_list[idxname]

            if len(docs_list) > 100:
                for topicid, topiclist in topicdict.items():
                    msg1 = "\n\nTOPIC:" + str(topicid) + "\n"
                    log.getLogger().debug(msg1)
                    term_list = []
                    for entry in topiclist:
                        term_list.append(str(entry[0]))
                    topics_as_terms[topicid] = term_list
                topics_as_terms_by_idx[idxname] = topics_as_terms

        return topics_as_terms_by_idx

    def _iterate_similar_topics(self, prepared_data):
        similarity_threshold = env.config.getint('ml_instructions', 'gensim_lda_topic_similarity_threshold')
        dict_all_topic_matches = {}
        for index_name, topics in prepared_data.items():


            for topic_id, topic_term_list in topics.items():
                dict_key = index_name + "_" + str(topic_id)
                dict_all_topic_matches[dict_key] = self._find_matching_topics(prepared_data,topic_term_list, index_name, similarity_threshold)
        return dict_all_topic_matches

    def _find_matching_topics(self, prepared_data, current_topic_term_list, current_index_name ,similarity_threshold):
        topic_match_dict = {}
        for index_name, topics in prepared_data.items():
            topic_match_list = []
            for topic_id, topic_term_list in topics.items():
                result = set(topic_term_list).intersection(current_topic_term_list)
                log.getLogger().debug("Similarity Count " +str(result))
                if len(result) >= similarity_threshold:
                    if not current_index_name == index_name:
                        topic_match_list.append(index_name)
            if len(topic_match_list) > 0:
                topic_match_dict[index_name] = topic_match_list
        return topic_match_dict




class GensimTopicReduction:

    def __init__(self):
        pass

    def perform(self, package:merm_model.PipelinePackage):
        #scipy_csc_matrix = gensim.matutils.corpus2csc(package.corpus)
        log.getLogger().info("STAGE: Seeking to reduce topics to those specified in input flatfile")
        csv = package.dependencies_dict["env"].config["local_data"]["confluence_lda_bysubset"]
        df = pd.read_csv(csv)
        df.dropna(inplace=True)
        reduced_topics = df.to_dict(orient="records")
        prepared_reduced_topics = self._prepare_reduced_topics(reduced_topics)

        prepare_data = self._prepare_data(package)
        matching_topics = self._iterate_similar_topics(prepare_data, prepared_reduced_topics)
        package.any_analysis_dict[package.default_analysis_key()] = matching_topics
        return merm_model.PipelinePackage(package.model,
                                          package.corpus,
                                          package.dict,
                                          package.linked_document_list,
                                          package.any_analysis_dict,
                                          package.any_inputs_dict,
                                          package.dependencies_dict)

    def load_top_level_topic_csv(self):
        file = env.config["ml_instructions"]["gensim_top_level_topic_csv"]
        top_level_topics = csv.read(file)
        top_level_topics


    def _prepare_data(self, package:merm_model.PipelinePackage):
        topics_as_terms_by_idx = {}
        for idxname, topicdict in package.any_analysis().items():
            topics_as_terms = {}
            msg = "\n\n\n+++++++++++++++++++\n\nSimilarity Search " + idxname + "\n\n"
            log.getLogger().debug(msg)
            docs_list = package.linked_document_list[idxname]

            if len(docs_list) > 100:
                for topicid, topiclist in topicdict.items():
                    msg1 = "\n\nTOPIC:" + str(topicid) + "\n"
                    log.getLogger().debug(msg1)
                    term_list = []
                    for entry in topiclist:
                        term_list.append(str(entry[0]))
                    topics_as_terms[topicid] = term_list
                topics_as_terms_by_idx[idxname] = topics_as_terms

        return topics_as_terms_by_idx

    def _prepare_reduced_topics(self, reduced_topics):


        prepared_reduced_topics_dict = {}
        for row_dict in reduced_topics:
            new_key = row_dict["index_name"] + "_" + str(row_dict["topic_id"])
            if new_key in prepared_reduced_topics_dict:
                prepared_reduced_topics_dict[new_key].append(row_dict["term"])

            else:
                term_list = []
                prepared_reduced_topics_dict[new_key] = term_list
                prepared_reduced_topics_dict[new_key].append(row_dict["term"])
        return prepared_reduced_topics_dict





    def _iterate_similar_topics(self, prepared_data, prepared_reduced_topics):
        similarity_threshold = env.config.getint('ml_instructions', 'gensim_lda_topic_similarity_threshold')
        topic_match_dict = {}
        for index_name_topic, reduced_topics in prepared_reduced_topics.items():
            topic_match_list = []
            for index_topic_key, topic_term_dict in prepared_data.items():
                for topic_id, topic_term_list in topic_term_dict.items():
                    result = set(reduced_topics).intersection(topic_term_list)
                    log.getLogger().debug("Similarity Count " +str(result))
                    if len(result) >= similarity_threshold:
                        if index_topic_key not in index_name_topic and index_topic_key not in topic_match_list:
                            topic_match_list.append(index_topic_key)
                if len(topic_match_list) > 0:
                    topic_match_dict[index_name_topic] = {"terms": reduced_topics, "matching_channels" : topic_match_list}
        return topic_match_dict
