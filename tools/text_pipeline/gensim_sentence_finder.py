from typing import List
from typing import Tuple
import tools.model.model_classes as merm_model
import tools.utils.text_parsing_utils as parser
import re

class GensimSentenceFinder:

    def __init__(self):
        pass

    def split_major(self, major_string):
        if "_" in major_string:
            as_list = major_string.split("_")
            rank = int(as_list.pop())
            major = ""
            for t in as_list:
                major = major + t
            return (major, rank)
        else:
            return (major_string, -1)

    def perform(self, package: merm_model.PipelinePackage):
        env = package.dependencies_dict["env"]
        report_dir = env.config["job_instructions"]["output_folder"]
        lda_all_groups = {}
        csv_list_of_lists = []
        csv_list_of_lists.append(["text_source", "major", "topic_id", "term", "weight"])
        report_sentences = env.config.getboolean('ml_instructions', 'gensim_lda_report_sentence_level')
        # analysis_key = gensim_grouped.analysis_output_label(package)
        minimum_doc_count = package.dependencies_dict["env"].config.getint('ml_instructions', 'minimum_doc_count')
        for corpus_name, topic_nalysis_dict in package.any_analysis_dict.items():
            if "Gensim_LDA" in corpus_name:
                corpus_dict = {}
                for majorid, majortopics in topic_nalysis_dict.items():
                    major_dict = {}
                    topic_dict = {}
                    if majorid not in major_dict.keys():
                        major_dict[majorid] = []


                    for topic, topiclist in majortopics.items():
                        if topic not in topic_dict.keys():
                            topic_dict[topic] = []
                        salient_sentences = self.find_salient_sentences(topiclist, package, majorid)
                        topic_dict[topic] = salient_sentences
                    corpus_dict[majorid] = topic_dict
                lda_all_groups[corpus_name] = corpus_dict
        package.log_stage("Identified salient sentences for each LDA topic")
        package.any_analysis_dict["lda_all_groups"] = lda_all_groups
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

        return_dict = {}

        while (len(topiclist) > 3):
            anchor = topiclist.pop()[0]
            idx = 0
            while idx < len(topiclist) :
                idxplus = idx + 1
                if idxplus < len(topiclist):
                    words3 = [anchor, topiclist[idx][0], topiclist[idxplus][0]]
                    salient_sentences = self.words_in_sentence_list(words3, sentences)
                    self.add_to_sentence_dict(return_dict,salient_sentences,words3)

                words2 = [anchor, topiclist[idx][0]]
                salient_sentences = self.words_in_sentence_list(words2, sentences)
                self.add_to_sentence_dict(return_dict, salient_sentences, words2)


                idx = idx + 1


        return return_dict


    def add_to_sentence_dict(self, return_dict, sentences, words):
        if len(sentences) > 0:
            return_dict[str(words)] = sentences


    def words_in_sentence_list(self, tokens: List[str], sentences):
        raw_sentences = []
        for sentence in sentences:
            all_tokens_found = True
            sentence_words = sentence.lower().split(" ")
            for token in tokens:
                if not token in sentence_words:
                    all_tokens_found = False
            if all_tokens_found == True:
                raw_sentences.append(sentence)
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

