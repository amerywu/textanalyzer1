import tools.model.model_classes as merm_model
import tools.utils.log as log
import tools.utils.envutils as env
import statistics as stats

class LinkedDocCorpusWordCount:

    def __init__(self):
        pass

    def count_as_doc_list(self, linked_document_list):
        corpus_word_frequency = {}
        for linked_doc in linked_document_list:
            doc_word_count = {}
            for token in linked_doc.tokens:
                if token in doc_word_count.keys():
                    doc_word_count[token] = doc_word_count[token] + 1
                else:
                    doc_word_count[token] = 1
            linked_doc.any_analysis["doc_word_count"] = doc_word_count

            for key in doc_word_count.keys():
                if key in corpus_word_frequency.keys():
                    corpus_word_frequency[key] = corpus_word_frequency[key] + doc_word_count[key]
                else:
                    corpus_word_frequency[key] = doc_word_count[key]
        return corpus_word_frequency

    def count_as_doc_dict(self, linked_document_dict):
        corpus_word_frequency = {}
        for linked_document_list in linked_document_dict.values():
            for linked_doc in linked_document_list:
                doc_word_count = {}
                for token in linked_doc.tokens:
                    if token in doc_word_count.keys():
                        doc_word_count[token] = doc_word_count[token] + 1
                    else:
                        doc_word_count[token] = 1
                linked_doc.any_analysis["doc_word_count"] = doc_word_count

                for key in doc_word_count.keys():
                    if key in corpus_word_frequency.keys():
                        corpus_word_frequency[key] = corpus_word_frequency[key] + doc_word_count[key]
                    else:
                        corpus_word_frequency[key] = doc_word_count[key]
        return corpus_word_frequency


    def perform(self,package:merm_model.PipelinePackage):
        if type(package.linked_document_list) is list:
            corpus_word_frequency = self.count_as_doc_list(package.linked_document_list)
        else:
            corpus_word_frequency = self.count_as_doc_dict(package.linked_document_list)

        package.any_analysis_dict["corpus_word_frequency"] = corpus_word_frequency
        count = str(len(corpus_word_frequency))
        total_word_count = str(sum(list(corpus_word_frequency.values())))
        mx = str(max(list(corpus_word_frequency.values())))
        median = str(stats.median(list(corpus_word_frequency.values())))
        stdev = str(stats.stdev(list(corpus_word_frequency.values())))
        doc_count = str(len(package.linked_document_list))

        log_string = "\nDocument count: " + doc_count + \
                     "\nTotal_word count: " + total_word_count + \
                     "\nUnique_word count: " + count + \
            "\nMax Frequency: " + mx + \
            "\nMedian Frequency: " + median + \
            "\nstdev: " + stdev

        package.log_stage(log_string)
        return package


class LinkedDocCorpusStopWordGenerator:

    def __init__(self):
        pass

    def perform(self,package:merm_model.PipelinePackage):
        colutils = package.dependencies_dict["colutils"]

        if "corpus_word_frequency" not in package.any_analysis_dict.keys():
            word_counter = LinkedDocCorpusWordCount()
            package = word_counter.perform(package)
        corpus_word_frequency = package.any_analysis_dict["corpus_word_frequency"]
        total_word_count = str(sum(list(corpus_word_frequency.values())))
        unique_word_count = str(len(list(corpus_word_frequency.keys())))
        stop_words_global = package.dependencies_dict["utils"]._stop_word_list_generator(package)
        stop_words_top_tuple = self._top_threshold(package)
        stop_words_top = stop_words_top_tuple[0]
        lowest_freq_at_top = stop_words_top_tuple[1]
        stop_words_bottom_tuple = self._bottom_threshold(package)
        stop_words_bottom = stop_words_bottom_tuple[0]
        max_freq_at_bottom = stop_words_bottom_tuple[1]
        stop_words = stop_words_bottom + stop_words_top +  stop_words_global
        analysis_key = colutils.incrementing_key("stop_words", package.any_analysis_dict)
        package.any_analysis_dict[analysis_key] = stop_words
        self.save_to_file(stop_words,package)
        package.log_stage("Generated stop words. \nGlobal stop word count: " + str(len(stop_words_global)) + "\nHigh frequency dynamically generated stop words: " + \
                          str(len(stop_words_top)) + "\nLow frequency dynamically generated stop words: " + str(len(stop_words_bottom)) +"\nLowest frequency: " + str(max_freq_at_bottom) + \
                          "\nHighest frequency: " + str(lowest_freq_at_top) + "\nOriginal word unique count is " + str(unique_word_count) + "\nTotal word count is " + str(total_word_count))
        return package

    def save_to_file(self, stop_words_new, package):
        stop_words = stop_words_new + self.standard_stopwords(package)
        corpus_name = package.any_inputs_dict["corpus_name"]
        output_text = ""
        stop_words_path = package.dependencies_dict["env"].config["job_instructions"]["stop_list_folder"] + str(corpus_name) +"_stopwords"
        for word in stop_words:
            output_text = output_text + word + "\n"
        package.dependencies_dict["env"].overwrite_file(stop_words_path, output_text)

    def standard_stopwords(self, package):
        stop_words_path = package.dependencies_dict["env"].config["job_instructions"]["stop_list"]
        stop_words_text = package.dependencies_dict["env"].read_file(stop_words_path)
        stop_words = stop_words_text.split("\n")
        return stop_words

    def _doc_word_count(self, linked_doc):
        doc_word_count = {}
        for token in linked_doc.tokens:
            if token in doc_word_count.keys():
                doc_word_count[token] = doc_word_count[token] + 1
            else:
                doc_word_count[token] = 1
        return doc_word_count

    def _corpus_word_frequency(self, doc_word_count, corpus_word_frequency):

        for key in doc_word_count.keys():
            if key in corpus_word_frequency.keys():
                corpus_word_frequency[key] = corpus_word_frequency[key] + doc_word_count[key]
            else:
                corpus_word_frequency[key] = doc_word_count[key]


    def _top_threshold(self, package:merm_model.PipelinePackage):
        corpus_word_frequency = package.any_analysis_dict["corpus_word_frequency"]
        total_words = sum(list(corpus_word_frequency.values()))
        stop_words = []
        lowest_freq = 10000000
        for key in corpus_word_frequency.keys():
            freq = corpus_word_frequency[key]
            proportion = freq / total_words
            top_threshold = package.dependencies_dict["env"].config.getfloat("ml_instructions","stopword_top_threshold")

            if proportion > top_threshold:
                stop_words.append(key)
                if freq < lowest_freq:
                    lowest_freq = freq

        return (stop_words, lowest_freq)




    def _bottom_threshold(self, package:merm_model.PipelinePackage):
        corpus_word_frequency = package.any_analysis_dict["corpus_word_frequency"]
        total_words = sum(list(corpus_word_frequency.values()))
        stop_words = []
        max_freq = 0
        for key in corpus_word_frequency.keys():
            freq = corpus_word_frequency[key]
            proportion = freq / total_words
            bottom_threshold = package.dependencies_dict["env"].config.getfloat("ml_instructions","stopword_bottom_threshold")
            if proportion < bottom_threshold:
                stop_words.append(key)
                if freq > max_freq:
                    max_freq = freq

        return (stop_words, max_freq)












