import tools.model.model_classes as merm_model
import tools.utils.log as log
import tools.utils.envutils as env

class LinkedDocCorpusWordCount:

    def __init__(self):
        pass

    def perform(self,package:merm_model.PipelinePackage):
        corpus_word_count = {}
        for linked_doc in package.linked_document_list:
            doc_word_count = {}
            for token in linked_doc.tokens:
                if token in doc_word_count.keys():
                    doc_word_count[token] = doc_word_count[token] + 1
                else:
                    doc_word_count[token] = 1
            linked_doc.any_analysis["doc_word_count"] = doc_word_count

            for key in doc_word_count.keys():
                if key in corpus_word_count.keys():
                    corpus_word_count[key] = corpus_word_count[key] + doc_word_count[key]
                else:
                    corpus_word_count[key] = doc_word_count[key]
        package.any_analysis_dict["corpus_word_count"] = corpus_word_count

        return package


class LinkedDocCorpusStopWordGenerator:

    def __init__(self):
        pass

    def perform(self,package:merm_model.PipelinePackage):
        colutils = package.dependencies_dict["colutils"]
        corpus_word_count = {}
        for linked_doc in package.linked_document_list:
            doc_word_count = self._doc_word_count(linked_doc)
            linked_doc.any_analysis["doc_word_count"] = doc_word_count
            self._corpus_word_count(doc_word_count,corpus_word_count)


        package.any_analysis_dict["corpus_word_count"] = corpus_word_count
        stop_words_global = package.dependencies_dict["utils"]._stop_word_list_generator(package)
        stop_words_top = self._top_threshold(package)
        stop_words_bottom = self._bottom_threshold(package)
        stop_words = stop_words_bottom + stop_words_top +  stop_words_global
        analysis_key = colutils.incrementing_key("stop_words", package.any_analysis_dict)
        package.any_analysis_dict[analysis_key] = stop_words
        self.save_to_file(stop_words,package)
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

    def _corpus_word_count(self, doc_word_count, corpus_word_count):

        for key in doc_word_count.keys():
            if key in corpus_word_count.keys():
                corpus_word_count[key] = corpus_word_count[key] + doc_word_count[key]
            else:
                corpus_word_count[key] = doc_word_count[key]


    def _top_threshold(self, package:merm_model.PipelinePackage):
        corpus_word_count = package.any_analysis_dict["corpus_word_count"]
        total_words = sum(list(corpus_word_count.values()))
        stop_words = []
        for key in corpus_word_count.keys():
            proportion = corpus_word_count[key] / total_words
            top_threshold = package.dependencies_dict["env"].config.getfloat("ml_instructions","stopword_top_threshold")
            if proportion > top_threshold:
                stop_words.append(key)

        return stop_words

    def _bottom_threshold(self, package:merm_model.PipelinePackage):
        corpus_word_count = package.any_analysis_dict["corpus_word_count"]
        total_words = sum(list(corpus_word_count.values()))
        stop_words = []
        for key in corpus_word_count.keys():
            proportion = corpus_word_count[key] / total_words
            bottom_threshold = package.dependencies_dict["env"].config.getfloat("ml_instructions","stopword_bottom_threshold")
            if proportion < bottom_threshold:
                stop_words.append(key)

        return stop_words












