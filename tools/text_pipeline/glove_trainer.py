
import tools.model.model_classes as data_models
import glove
import copy
import numpy as np
from scipy import sparse
import time
import tools.utils.log as log
# pip3 install https://github.com/JonathanRaiman/glove/archive/master.zip

class GloveModelBuilder:

    def __init__(self):
        pass

    def _tokens_list_of_lists(self, package:data_models.PipelinePackage):
        tokens_corpora =[]
        for doc in package.linked_document_list:
            tokens_corpora.append(doc.tokens)
        return  tokens_corpora



    def perform(self, package:data_models.PipelinePackage):
        corpus = self._tokens_list_of_lists(package)



        vocab = self.build_vocab(corpus)
        env = package.dependencies_dict["env"]
        alpha = env.config.getfloat("ml_instructions", "glove_alpha")
        x_max = env.config.getfloat("ml_instructions", "glove_x_max")
        dimensions =  env.config.getint("ml_instructions", "glove_dimensions")
        window =  env.config.getint("ml_instructions", "glove_window")
        corpus_doc_count = len(package.linked_document_list)
        unique_word_count  = len(vocab)

        cooccur_start = time.time()
        cooccurrences = self.build_cooccur(vocab, corpus, window_size=window)
        cooccur_time = (time.time() - cooccur_start)
        log.getLogger().info("cooccur time " + str(cooccur_time))
        cooccurrence_dict = self.format_for_glove(cooccurrences)

        glove_start = time.time()

        model = glove.Glove(cooccurrence_dict, d=dimensions, alpha=alpha, x_max=x_max)
        glove_time = (time.time() - glove_start)
        log.getLogger().info("glove_time  " + str(glove_time))
        glove_train_start = time.time()
        model.train(batch_size=200, workers=9)

        glove_train_time = (time.time() - glove_train_start)
        log.getLogger().info("glove_train_time  " + str(glove_train_time))
        glove_list = self.output_format(model.W, vocab)
        package.any_analysis_dict[str(dimensions)+"d_glove_output"] = glove_list
        package.any_analysis_dict["glove_vocab"] = vocab
        package.log_stage("gLoVe model builder: \nunique words: " + str(unique_word_count) +"\ndoc count: " + str(corpus_doc_count) + "\nwindow: " + str(window) + \
                          "\nalpha: " + str(alpha) + "\ndimensions: " + str(dimensions) + "\nx_max: " + str(x_max) + \
                          "\ncooccur_time: " + str(cooccur_time) + "\nglove_time: " + str(glove_time) + "\nglove_train_time: " + str(glove_train_time))
        return package


    def output_format(self, modelW, vocab_dict):
        vocab_idx = 0
        inverted_dict = {value: key for key, value in vocab_dict.items()}
        glove_list_of_lists = []
        for row in modelW:
            row_list = []
            row_list.append(inverted_dict[vocab_idx])
            for cell in row:
                row_list.append(cell)
            vocab_idx = vocab_idx + 1
            glove_list_of_lists.append(row_list)
        return glove_list_of_lists


    def build_cooccur(self, vocab, corpus, window_size=3, min_count=None):
        vocab_size = len(vocab)
        # id2word = dict((i, word) for word, (i, _) in vocab.items())

        # Collect cooccurrences internally as a sparse matrix for
        # passable indexing speed; we'll convert into a list later
        cooccurrences = sparse.lil_matrix((vocab_size, vocab_size),
                                          dtype=np.float64)
        for i, line in enumerate(corpus):
            tokens = line
            # token_ids = [vocab[word][0] for word in tokens]
            token_ids = [vocab[word] for word in tokens]
            for center_i, center_id in enumerate(token_ids):
                # Collect all word IDs in left window of center word
                context_ids = token_ids[max(0, center_i - window_size)
                                        : center_i]
                contexts_len = len(context_ids)

                for left_i, left_id in enumerate(context_ids):
                    # Distance from center word
                    distance = contexts_len - left_i

                    # Weight by inverse of distance between words
                    increment = 1.0 / float(distance)

                    # Build co-occurrence matrix symmetrically (pretend
                    # we are calculating right contexts as well)
                    cooccurrences[center_id, left_id] += increment
                    cooccurrences[left_id, center_id] += increment
        return cooccurrences

    def build_vocab(self, corpus):
        vocab = {}
        count = 0
        for line in corpus:
            for word in line:
                if word not in vocab.keys():
                    vocab[word] = count
                    count = count + 1
        return vocab

    def format_for_glove(self, cooccur):
        cooccurrence_dict = {}

        for i, (row, data) in enumerate(zip(cooccur.rows, cooccur.data)):
            word_dict = {}
            for j, val in zip(row, data):
                cooccur[i, j] = val
                word_dict[j] = val
            cooccurrence_dict[i] = word_dict
        return cooccurrence_dict

class GloveLoadings:

    def __init__(self):
        pass

    def perform(self, package:data_models.PipelinePackage):

        env = package.dependencies_dict["env"]
        report_count = env.config.getint("ml_instructions", "glove_loadings_count_to_report")
        dimensions = env.config.getint("ml_instructions", "glove_dimensions")

        glove_model = package.any_analysis_dict[str(dimensions)+"d_glove_output"]
        vocab = package.any_analysis_dict["glove_vocab"]
        inverted_dict = {value: key for key, value in vocab.items()}
        as_col_dict = {}
        x = range(len(glove_model[0])-1)
        for n in x:
            as_col_dict[n] = []



        for row_idx, row_real in enumerate(glove_model):
            row = row_real.copy()
            word_idx = vocab[row.pop(0)]
            for col_idx, loading in enumerate(row):
                as_col_dict[col_idx].append([word_idx,loading])

        final_list = []
        for vectore_id, vector in as_col_dict.items():
            vector.sort(key=lambda tup: tup[1])
            top_list = vector[:report_count]
            bottom_list = vector[-report_count:]
            top_bottom_list = top_list + bottom_list
            new_list = []
            for tuply_thing in top_bottom_list:
                new_list.append([str(vectore_id) +","+ str(inverted_dict[tuply_thing[0]]) +"," + str(tuply_thing[1])])

            final_list.append(new_list)


        package.any_analysis_dict[str(dimensions)+"d_glove_biggest_loadings"] = final_list
        package.log_stage("GloveLoadings: ")
        return package
