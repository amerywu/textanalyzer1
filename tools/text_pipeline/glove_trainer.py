
import tools.model.model_classes as data_models
import glove
import copy
import numpy as np
from scipy import sparse
import time
import tools.utils.log as log
import statistics as stats

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
        alpha_list = env.config["ml_instructions"] ["glove_alpha"].split(",")
        alpha_list = [float(i) for i in alpha_list]
        x_max_list = env.config["ml_instructions"] ["glove_x_max"].split(",")
        x_max_list = [float(i) for i in x_max_list]
        dimensions_list =  env.config["ml_instructions"]["glove_dimensions"].split(",")
        dimensions_list = [int(i) for i in dimensions_list]

        window =  env.config.getint("ml_instructions", "glove_window")
        corpus_doc_count = len(package.linked_document_list)
        unique_word_count  = len(vocab)

        cooccur_start = time.time()
        cooccurrences = self.build_cooccur(vocab, corpus, window_size=window)
        cooccur_time = (time.time() - cooccur_start)
        log.getLogger().info("cooccur time " + str(cooccur_time))
        cooccurrence_dict = self.format_for_glove(cooccurrences)
        package.any_analysis_dict["cooccurrence_stats"] = self._cooccur_descriptives(cooccurrence_dict)


        for alpha in alpha_list:
            for x_max in x_max_list:
                for dimensions in dimensions_list:
                    self._do_glove(package, cooccurrence_dict, dimensions, alpha, x_max, vocab)


        package.log_stage("\ncooccurrences: average: " + str(package.any_analysis_dict["cooccurrence_stats"]["average"]) + "\ncooccurrences: median: " + str(package.any_analysis_dict["cooccurrence_stats"]["median"])  + \
                        "\ncooccurrences: stdev: " + str(package.any_analysis_dict["cooccurrence_stats"]["stdev"])  + \
                        "\ngLoVe model builder: \nunique words: " + str(unique_word_count) +"\ndoc count: " + str(corpus_doc_count) + "\nwindow: " + str(window) + \
                        "\nalphas: " + str(alpha_list) + "\ndimensions_list: " + str(dimensions_list) + "\nx_max_list: " + str(x_max_list) )

        return package

    def _cooccur_descriptives(self, cooccurrence_dict):
        all_values_list = []
        for word_list in cooccurrence_dict.values():
            for cooccur in word_list.values():
                all_values_list.append(cooccur)

        average = stats.mean(all_values_list)
        median = stats.median(all_values_list)
        stdev = stats.stdev(all_values_list)
        return_dict = {}
        return_dict["average"] = average
        return_dict["median"] = median
        return_dict["stdev"] = stdev
        return return_dict



    def _do_glove(self, package, cooccurrence_dict, dimensions, alpha, x_max, vocab):
        glove_start = time.time()
        model = glove.Glove(cooccurrence_dict, d=dimensions, alpha=alpha, x_max=x_max)
        glove_time = (time.time() - glove_start)
        log.getLogger().info("glove_time  " + str(glove_time))
        glove_train_start = time.time()
        model.train(batch_size=200, workers=9)
        glove_train_time = (time.time() - glove_train_start)
        log.getLogger().info("glove_train_time  " + str(glove_train_time))
        glove_list = self.output_format(model.W, vocab)
        glove_output_key = str(dimensions) + "d_" + str(x_max) + "_" + str(alpha) + "_glove_output"

        if "glove_output_key" in package.any_inputs_dict.keys():
            package.any_inputs_dict["glove_output_key"] = package.any_inputs_dict["glove_output_key"] + "," + glove_output_key
        else:
            package.any_inputs_dict["glove_output_key"] = glove_output_key

        package.any_analysis_dict[glove_output_key] = glove_list
        package.any_analysis_dict["gl0ve_vocab"] = vocab



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
        self.variance_dict = []
        pass

    def perform(self, package:data_models.PipelinePackage):

        env = package.dependencies_dict["env"]
        report_count = env.config.getint("ml_instructions", "glove_loadings_count_to_report")


        glove_output_key_list  = package.any_inputs_dict["glove_output_key"].split(",")
        for glove_output_key in glove_output_key_list:
            self._process_loadings(package,glove_output_key, report_count)

        package.log_stage("GloveLoadings: ")
        package.any_analysis_dict["glove_variance"] = self.variance_dict
        return package

    def _generate_col_dict(self, glove_model, vocab):
        as_col_dict = {}
        x = range(len(glove_model[0]) - 1)
        for n in x:
            as_col_dict[n] = []

        for row_idx, row_real in enumerate(glove_model):
            row = row_real.copy()
            word_idx = vocab[row.pop(0)]
            for col_idx, loading in enumerate(row):
                as_col_dict[col_idx].append([word_idx, loading])
        return as_col_dict

    def _process_variances(self, as_col_dict, glove_output_key):

        uber_list = []

        variance_string = glove_output_key + ","
        for vector_id, vector in as_col_dict.items():
            unzipped = [ j for i, j in vector ]
            variance = stats.variance(unzipped)
            variance_string = variance_string + str('%.8f'%(variance)) + ","
        uber_list.append(variance_string)

        self.variance_dict.append(uber_list)

    def _process_loadings(self, package, glove_output_key, report_count ):
        glove_model = package.any_analysis_dict[glove_output_key]
        vocab = package.any_analysis_dict["gl0ve_vocab"]
        inverted_dict = {value: key for key, value in vocab.items()}
        as_col_dict =  self._generate_col_dict(glove_model, vocab)
        vocab_freq = package.any_analysis_dict["corpus_word_frequency"]
        final_list = []
        self._process_variances(as_col_dict,glove_output_key)

        for vectore_id, vector in as_col_dict.items():
            vector.sort(key=lambda tup: tup[1])
            top_list = vector[:report_count]
            bottom_list = vector[-report_count:]
            top_bottom_list = top_list + bottom_list
            new_list = []
            for tuply_thing in top_bottom_list:
                word = inverted_dict[tuply_thing[0]]
                word_frq = vocab_freq[word]
                new_list.append([str(vectore_id) + "," + word + "," + str(tuply_thing[1]) + "," + str(abs(tuply_thing[1])) + "," + str(word_frq)])
            final_list.append(new_list)

        package.any_analysis_dict[glove_output_key + "_biggest_loadings"] = final_list
