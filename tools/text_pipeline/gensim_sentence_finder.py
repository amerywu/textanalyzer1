from typing import List
from typing import Tuple
import tools.model.model_classes as merm_model
import tools.utils.text_parsing_utils as parser
import re

class GensimSentenceFinder:

    def __init__(self):
        pass

    def perform(self, package: merm_model.PipelinePackage):
        env = package.dependencies_dict["env"]
        report_dir = env.config["job_instructions"]["output_folder"]

        csv_list_of_lists = []
        csv_list_of_lists.append(["text_source", "major", "topic_id", "term", "weight"])
        report_sentences = env.config.getboolean('ml_instructions', 'gensim_lda_report_sentence_level')
        # analysis_key = gensim_grouped.analysis_output_label(package)
        minimum_doc_count = package.dependencies_dict["env"].config.getint('ml_instructions', 'minimum_doc_count')
        for idxname, topicdict in package.any_analysis_dict.items():
            if "Gensim_LDA" in idxname:
                for majorid, majortopics in topicdict.items():
                    report_for_index = "\n\n\n+++++++++++++++++++\n\nReport for " + idxname + "_" + majorid + "\n\n"
                    # if report_sentences == True:
                    #     corpus_as_sentences = break_corpus_as_sentences(docs_list)
                    # report_for_index += "Corpus Size: " + str(len(docs_list)) + "\n"

                    doc_count = len(self.get_text_rank_sentences(package, majorid))
                    if doc_count >= minimum_doc_count:
                        for topic, topiclist in majortopics.items():
                            salient_sentences = self.find_salient_sentences(topiclist, package, majorid)
                            report_for_index += "\n\nSALIENT_SENTENCES\n"
                            for sentence in salient_sentences:
                                report_for_index += sentence + "\n"
        package.log_stage("Identified salient sentences for each LDA topic")
        return package



    def break_corpus_as_sentences(self, docs_list: List[merm_model.LinkedDocument]):
        corpus_as_sentences = []
        for doc in docs_list:
            corpus_as_sentences = corpus_as_sentences + parser.split_linked_doc_by_sentence(doc)
        corpus_as_sentences = parser.tokenize(corpus_as_sentences)
        corpus_as_sentences = parser.lemmatize_tokens(corpus_as_sentences, parser.standard_stop_words())
        return corpus_as_sentences

    def find_salient_sentences(self, topiclist: List[Tuple[str, int]], package, topicid):
        all_sentences = self.get_text_rank_sentences(package, topicid)
        return self.compile_sentences(all_sentences, topiclist)

    def compile_sentences(self, sentences, topiclist):
        raw_sentences = []
        top_words_indices = [0, 1, 2, 3, 4, 5, 6]

        bottomidx = 0
        while (bottomidx < len(top_words_indices)):
            topidx = bottomidx + 2
            while topidx < len(top_words_indices):
                words = [topiclist[bottomidx][0], topiclist[topidx - 1][0], topiclist[topidx][0]]
                raw_sentences = raw_sentences + self.words_in_sentence_list(words, sentences)
                topidx = topidx + 1
            bottomidx = bottomidx + 1

        if len(raw_sentences) < 15:
            bottomidx = 0
            while (bottomidx < len(top_words_indices)):
                topidx = bottomidx + 1
                while topidx < len(top_words_indices):
                    words = [topiclist[bottomidx][0], topiclist[topidx][0]]
                    raw_sentences = raw_sentences + self.words_in_sentence_list(words, sentences)
                    topidx = topidx + 1
                bottomidx = bottomidx + 1
        return raw_sentences

    def words_in_sentence_list(self, tokens: List[str], sentences):
        raw_sentences = []
        for sentence in sentences:
            all_tokens_found = True
            sentence_words = sentence.lower().split(" ")
            for token in tokens:
                if not token in sentence_words:
                    all_tokens_found = False
            if all_tokens_found == True:
                raw_sentences.append(str(tokens) + " ==> " + sentence)
        return raw_sentences

    def is_by_rank(self, topicid):
        regexp = re.compile(r'_[0-9]')
        return regexp.search(topicid)

    def get_text_rank_sentences(self, package: merm_model.PipelinePackage, topicid):
        docs_dict = package.any_analysis_dict["text_rank_all_groups"]
        topicid = topicid.replace("lemmatized", "")

        if self.is_by_rank(topicid):
            splittopic = topicid.split("_")
            idx = int(splittopic.pop())
            topic = ""
            for s in splittopic:
                topic = topic + s
            return docs_dict[topic][idx]
        else:
            return_list = []
            for sentences in docs_dict[topicid].values():
                return_list = return_list + sentences
            return return_list

