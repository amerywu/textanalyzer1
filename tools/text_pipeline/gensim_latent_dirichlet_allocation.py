import typing
import pprint
import tools.model.model_classes as merm_model
import tools.utils.log as log
import tools.utils.envutils as env
import gensim
import csv
import pandas as pd
from gensim import corpora


def lda_analysis_key( package: merm_model.PipelinePackage):
    return "Gensim_LDA_" + package.any_inputs_dict["previous_task"]

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
            topic_dict_friendly = {}
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
                words_for_topic_friendly = []
                for w in topic:
                    msg = str(index) + ":" + str(w)
                    log.getLogger().info(msg)
                    words_for_topic.append((w[0],w[1]))
                    words_for_topic_friendly.append(str(w[0]) + "," + str(w[1]))
                topic_dict[index] = words_for_topic
                topic_dict_friendly[index] = words_for_topic_friendly

            package.any_analysis_dict[lda_analysis_key(package)] = topic_dict
            package.any_analysis_dict[lda_analysis_key(package) + "_friendly"] = topic_dict_friendly
            new_package = merm_model.PipelinePackage(lda_model,package.corpus,package.dict,package.linked_document_list,package.any_analysis_dict, package.any_inputs_dict, package.dependencies_dict)
            new_package.log_stage("Performed Gensim LDA.\nTopic Count: " + str(topic_count) + "\nIterations: " + str(100) + \
                                  "\nalpha = 0 \nUpdate Every: 1\n per_word_topics: False\nReporting on top " + str(report_word_count) + "words in each topic\n")
            return new_package
        else:
            new_package = merm_model.PipelinePackage(None, package.corpus, package.dict,
                                                       package.linked_document_list, [], package.any_inputs_dict,
                                                     package.dependencies_dict)
            new_package.log_stage("Gensim LDA aborted. There were too few tokens")
            return new_package


class GensimLDADistinctTopics:

    def __init__(self):
        pass

    def perform(self, package:merm_model.PipelinePackage):
        #scipy_csc_matrix = gensim.matutils.corpus2csc(package.corpus)
        log.getLogger().info("STAGE: Running a standard LDA in Gensim")
        topic_count = env.config.getint('ml_instructions', 'gensim_lda_topics')
        permitted_overlap = env.config.getint('ml_instructions', 'gensim_lda_permitted_term_overlap_across_topics')


        log.getLogger().info("Seeking " + str(topic_count) + " topics")
        report_word_count = env.config.getint('ml_instructions', 'gensim_lda_term_per_topic_reporting_count')
        if len(package.dict.token2id) > 50:


            new_package = self._run_lda(topic_count,report_word_count, permitted_overlap, package)
            new_package.log_stage("Performed Gensim LDA.\nTopic Count: " + str(topic_count) + "\nIterations: " + str(100) + \
                                  "\nalpha = 0 \nUpdate Every: 1\n per_word_topics: False\nReporting on top " + str(report_word_count) + "words in each topic\n")
            return new_package
        else:
            new_package = merm_model.PipelinePackage(None, package.corpus, package.dict,
                                                       package.linked_document_list, [], package.any_inputs_dict,
                                                     package.dependencies_dict)
            new_package.log_stage("Gensim LDA aborted. There were too few tokens")
            return new_package


    def _run_lda(self, topic_count, report_word_count, permitted_overlap, package:merm_model.PipelinePackage):
        topic_dict = {}
        topic_dict_friendly = {}
        lda_model = gensim.models.ldamodel.LdaModel(corpus=package.corpus,
                                                    id2word=package.dict,
                                                    num_topics=topic_count,
                                                    update_every=1,
                                                    alpha='auto',
                                                    per_word_topics=False,
                                                    iterations=100)

        topics = lda_model.show_topics(formatted=False, num_words=report_word_count)
        for index, topic in topics:
            # print('Topic: {} \nWords: {}'.format(index, [w[0] for w in topic]))
            words_for_topic = []
            words_for_topic_friendly = []
            for w in topic:
                words_for_topic.append((w[0], w[1]))
                words_for_topic_friendly.append(str(w[0]) + "," + str(w[1]))
            topic_dict[index] = words_for_topic
            topic_dict_friendly[index] = words_for_topic_friendly

        topic_overlap = self._topic_overlap(topic_dict)
        log.getLogger().info(str(topic_overlap))
        stop_words = self._dynamic_stop_words(topic_overlap, permitted_overlap)
        if len(stop_words) > permitted_overlap:
            log.getLogger().info("\n**********\nRerunning LDA after removing " + str(len(stop_words)) + " words")
            package = self._remove_stop_words(stop_words,package)
            package = self._rebuild_corpus(package)
            return self._run_lda(topic_count,report_word_count,permitted_overlap,package)
        package.any_analysis_dict[lda_analysis_key(package) + "_topic_overlap"] = topic_overlap
        package.any_analysis_dict[lda_analysis_key(package)] = topic_dict
        package.any_analysis_dict[lda_analysis_key(package) + "_friendly"] = topic_dict_friendly
        return package


    def _remove_stop_words(self, stop_words, package:merm_model.PipelinePackage):
        for linked_doc in package.linked_document_list:
            for word in stop_words:
                if word in linked_doc.tokens:
                    linked_doc.tokens.remove(word)
            linked_doc.raw = " ".join(linked_doc.tokens)
        return package

    def _rebuild_corpus(self, package:merm_model.PipelinePackage):
        linked_doc_list = package.linked_document_list
        log.getLogger().info(
            "Converting corpora as bag of words. Input format is List[List[str]]. Output is Gensim Dictionary")
        log.getLogger().info("Corpus size: " + str(len(package.linked_document_list)))
        bowlist = []
        for doc in linked_doc_list:
            bowlist.append(doc.tokens)

        dictionary = corpora.Dictionary(bowlist)

        # log.getLogger().info(dictionary)
        log.getLogger().info("Incoming doc count: " + str(len(linked_doc_list)))
        corpus = [dictionary.doc2bow(line) for line in bowlist]
        package.corpus = corpus
        package.dict = dictionary
        return package

    def _dynamic_stop_words(self, word_count_dict, permitted_overlap):

        stop_list = []
        for word, count in word_count_dict.items():
            if count > permitted_overlap:
                stop_list.append(word)
        return stop_list




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
