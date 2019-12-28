import tools.model.model_classes as merm_model
import tools.utils.log as log


class TextCleaner_Df_Corpus:

    def __init__(self):
        pass

    def perform(self, package: merm_model.PipelinePackage):
        keywords_by_document_dict = {}
        df = package.corpus
        text_fields = package.dependencies_dict["env"].config["ml_instructions"]["text_fields_to_clean"]
        text_fields_list = text_fields.split(",")
        env = package.dependencies_dict["env"]
        text_parsing_utils = package.dependencies_dict["utils"]
        colutils = package.dependencies_dict["colutils"]
        strip_nonalphanumeric = env.config.getboolean("ml_instructions", "strip_nonalphanumeric")
        strip_tags = env.config.getboolean("ml_instructions", "strip_tags")
        strip_short = env.config.getboolean("ml_instructions", "strip_short")
        strip_multispaces = env.config.getboolean("ml_instructions", "strip_multispaces")
        strip_punctuation = env.config.getboolean("ml_instructions", "strip_punctuation")
        split_alphanum = env.config.getboolean("ml_instructions", "split_alphanum")

        log.getLogger().info("Shape of DF: " + str(df.shape))

        clean_text_dict = {}
        for index, row in df.iterrows():
            for text_field in text_fields_list:

                document = row[text_field]
                sentences = self._split_to_sentences(document)
                key = "clean_" + text_field + row["id"]
                for sentence in sentences:
                    clean_sentence = text_parsing_utils.gensim_clean_string(sentence,strip_tags,split_alphanum,strip_nonalphanumeric,strip_multispaces,strip_short,3,strip_punctuation)

                    colutils._get_list_from_dict(clean_text_dict,key).append(clean_sentence)
                df.at[index, text_field] = colutils._list_to_string(colutils._get_list_from_dict(clean_text_dict,key))
        as_analysis = env.config.getboolean("ml_instructions", "put_cleaned_text_in_analysis_dict")
        if as_analysis == True:
            package.any_analysis_dict["cleaned_text"] = clean_text_dict
        return package




    def _split_to_sentences(self, document):
        if document is not None:

            return document.split(".")
        else:
            return ""


class Lemmatize_Corpus_LinkedDocs:

    def __init__(self):
        pass

    def perform(self, package: merm_model.PipelinePackage):
        text_utils = package.dependencies_dict["utils"]

        for linked_doc in package.linked_document_list:
            sentence_docs= text_utils.split_linked_doc_by_sentence(linked_doc)
            sentence_docs = text_utils.lemmatize_tokens(sentence_docs, text_utils.standard_stop_words())
            doc_string_lemmatized = ""
            for sentence in sentence_docs:
                sentence_string_lemmatized = ""
                for token in sentence.tokens:
                    sentence_string_lemmatized = sentence_string_lemmatized + str(token) + " "
                sentence_string_lemmatized = sentence_string_lemmatized + ". "
                doc_string_lemmatized = doc_string_lemmatized + sentence_string_lemmatized
            linked_doc.raw = doc_string_lemmatized


        return package


