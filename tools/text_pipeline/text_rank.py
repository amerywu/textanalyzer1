import tools.model.model_classes as merm_model
import tools.utils.log as log
import numpy as np
import pandas as pd
import networkx as nx
import nltk
from scipy.sparse import dok_matrix
nltk.download('punkt') # one time execution
import re
from sklearn.metrics.pairwise import cosine_similarity

class TextRank:

    def __init__(self):
        pass

    def perform(self, package: merm_model.PipelinePackage):
        word_embeddings_list = self._word_embeddings()
        utils = package.dependencies_dict["utils"]
        colutils = package.dependencies_dict["colutils"]
        #sentences = package.dependencies_dict["utils"].corpus_as_sentence_list(package)
        tokenized_sentences_by_doc = utils.corpus_as_tokenized_sentence_linked_doc_list_grouped_by_doc(package, True)
        log.getLogger().info("we have " + str(len(tokenized_sentences_by_doc)) + " docs")
        rank_by_dict = self._prep_rank_by_doc_dict(package)
        count = 0
        for docid, sentences in tokenized_sentences_by_doc.items():
            sentence_by_rank_dict = self.rank_by_document(sentences,word_embeddings_list, package)
            for key, value in sentence_by_rank_dict.items():
                sentence_list_for_that_rank = rank_by_dict[key]
                sentence_list_for_that_rank.append(value)
            if count % 100 == 0:
                print(count)
            count = count + 1
        analysis_key = colutils.incrementing_key("text_rank",package.any_analysis_dict)
        package.any_analysis_dict[analysis_key] = rank_by_dict
        return package

    def _prep_rank_by_doc_dict(self, package):
        rank_dict = {}
        top_n_rankings_count = package.dependencies_dict["env"].config.getint("ml_instructions",
                                                                              "text_rank_top_n_rankings")
        for i in range(top_n_rankings_count):
            rank_dict[i]  = []
        return rank_dict



    def rank_by_document(self, tokenized_sentences, word_embeddings_list, package):
        clean_sentences_list = []
        for linked_sentence_doc in tokenized_sentences:
            clean_sentences_list.append(linked_sentence_doc.raw)
        sentence_vectors = self.generate_sentence_vectors(clean_sentences_list, word_embeddings_list)


        scores = self._score_generator(clean_sentences_list,sentence_vectors)
        if len(scores) == 0:
            return {}
        else:
            ranked_sentences = sorted(((scores[i], s) for i, s in enumerate(clean_sentences_list)), reverse=True)
            top_n_rankings_count = package.dependencies_dict["env"].config.getint("ml_instructions", "text_rank_top_n_rankings")
            sentence_by_rank_dict = {}
            for i in range(top_n_rankings_count):
                if i < len(ranked_sentences):
                    #print(ranked_sentences[i][1])

                    sentence_by_rank_dict[i] = ranked_sentences[i][1]

            return sentence_by_rank_dict




    def generate_sentence_vectors(self, clean_sentences, word_embeddings):

        ############################################################################################
        # Now, let’s create vectors for our sentences. We will first fetch vectors (each of size 100 elements)
        # for the constituent words in a sentence and then take mean/average of those vectors to arrive at a
        # consolidated vector for the sentence.

        sentence_vectors = []
        for sentence in clean_sentences:
            if len(sentence) != 0:
                v = sum([word_embeddings.get(w, np.zeros((100,))) for w in sentence.split()]) / (len(sentence.split()) + 0.001)

            else:
                v = [np.zeros((100,))]
            #sentence_vectors.append(v)
        return sentence_vectors

    def _word_embeddings(self):
        ########################################################################################
        # we need to repreent words as vectors using word embedding
        # where words or phrases from the vocabulary are mapped to vectors of real numbers
        # We will be using the pre-trained Wikipedia 2014 + Gigaword 5 GloVe vectors available
        # that take words order into accout.
        # !get it http://nlp.stanford.edu/data/glove.6B.zip
        # !unzip glove*.zip

        word_embeddings = {}
        f = open('./resources/nlp_inputs/glove.6B.100d.txt', encoding='utf-8')
        for line in f:
            values = line.split()
            word = values[0]
            coefs = np.asarray(values[1:], dtype='float32')
            word_embeddings[word] = coefs
        f.close()
        return word_embeddings

    def _score_generator(self, sentences, sentence_vectors):
        sentence_count = len(sentences)
        similarity_matrix = dok_matrix((sentence_count, sentence_count), dtype=np.float32)
        for i in range(len(sentences)):
            for j in range(len(sentences)):
                if i != j and len(sentence_vectors) > i:
                    value = cosine_similarity(sentence_vectors[i].reshape(1, 100), sentence_vectors[j].reshape(1, 100))[0, 0]
                    similarity_matrix[i , j] = value

        # Before proceeding further, let’s convert the similarity matrix sim_mat into a graph. The nodes of this graph will
        # represent the sentences and the edges will represent the similarity scores between the sentences. On this graph,
        # we will apply the PageRank algorithm to arrive at the sentence rankings.

        try:
            nx_graph = nx.from_scipy_sparse_matrix(similarity_matrix)
            scores = nx.pagerank(nx_graph, max_iter = 200)
        except Exception as e:
            log.getLogger().error(str(e))
            return []

        return scores

class TextRankResultsToLinkedDocList:

    def __init__(self):
        pass

    def perform(self, package: merm_model.PipelinePackage):
        linked_doc_dict = {}
        package.cache_linked_docs()
        text_rank_all_groups = package.any_analysis_dict["text_rank_all_groups"]
        for key, item_dict in text_rank_all_groups.items():
            linked_doc_list = []
            for rank, item_list in item_dict.items():
                for sentence in item_list:
                    linked_doc = package.dependencies_dict["utils"].sentence_to_linked_doc(sentence)
                    linked_doc_list.append(linked_doc)
                linked_doc_dict[key + "_" +str(rank)] = linked_doc_list
        package.linked_document_list = linked_doc_dict
        return package



