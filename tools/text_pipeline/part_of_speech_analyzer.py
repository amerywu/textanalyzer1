import tools.model.model_classes as merm_model
import tools.utils.log as log
import tools.utils.envutils as env
import spacy

class PartOfSpeechAnalyzerFromTextRank:

    def __init__(self):
        pass


    def perform(self,package:merm_model.PipelinePackage):
        nlp = spacy.load('en_core_web_sm')
        colutils = package.dependencies_dict["colutils"]
        analysis_key = colutils.incrementing_key("noun_phrase", package.any_analysis_dict)
        all_ranks_results_dict = {}
        textrank_key = colutils.get_top_incrementing_key("text_rank", package.any_analysis_dict)
        textrank_dict = package.any_analysis_dict[textrank_key]

        for rank, sentence_list  in textrank_dict.items():
            all_ranks_results_dict[rank] = self._pos_for_linked_docs(sentence_list, nlp)



        package.any_analysis_dict[analysis_key] = all_ranks_results_dict
        package.log_stage("Used spacy nlp to generate noun phrases from the text rank results")
        return package

    def _reset_np(self, np_text, noun_phrases):
        if len(np_text) > 0:
            npf = np_text
            noun_phrases.append(npf.strip(" "))
        return ""


    def _pos_for_linked_docs(self, sentences:list, nlp):
        noun_tags = ["NN", "VBG", "ADP", "JJS", "JJ", "NNP", "DT", "CC", "NNS", "ADV", "RB", "PRP$", "IN"]
        np_list_final = []
        np_text = ""
        for sentence in sentences:
            doc = nlp(sentence)
            noun_phrases = []
            np_text = self._reset_np(np_text, noun_phrases)


            for token in doc:
                tag = token.tag_
                text = token.text
                if tag in noun_tags:
                    np_text = np_text + text + " "
                else:
                    if len(np_text) > 0:
                        np_text = self._reset_np(np_text, noun_phrases)


            np_list_final = np_list_final + noun_phrases
        return self._filter_np_list(np_list_final)

    def _filter_np_list(self, np_list_final):
        new_np_list = []
        for np in np_list_final:
            if len(np) > 12:
                new_np_list.append(np)
        return new_np_list




class LemmatizeTokens:

    def __init__(self):
        pass


    def perform(self,package:merm_model.PipelinePackage):
        env = package.dependencies_dict
        text_utils = env["utils"]
        syntax = env["syntax"]
        syntax.lemmatize_tokens(package.linked_document_list, text_utils.standard_stop_words())
        package.log_stage("lemmatized tokens")
        return package

class LemmatizeDocs:

    def __init__(self):
        pass


    def perform(self,package:merm_model.PipelinePackage):
        env = package.dependencies_dict
        text_utils = env["utils"]
        syntax = env["syntax"]
        syntax.lemmatize_docs(package.linked_document_list, text_utils.standard_stop_words())
        package.log_stage("lemmatized tokens")
        return package




