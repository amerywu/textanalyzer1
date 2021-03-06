import re
from typing import List


from gensim.parsing.preprocessing import strip_non_alphanum
from gensim.parsing.preprocessing import strip_punctuation

from gensim.parsing.preprocessing import split_alphanum
from gensim.parsing.preprocessing import strip_multiple_whitespaces
from gensim.parsing.preprocessing import strip_short
from gensim.parsing.preprocessing import strip_tags
import tools.model.model_classes as merm_model
import tools.utils.envutils as env
import tools.utils.log as log
import tools.model.model_classes as data_models



def clean_string_for_tokenizing(textIn):
    cleaned = gensim_clean_string(textIn)
    cleaned = cleaned.replace("'s", "")
    cleaned = cleaned.replace("'", "")
    cleaned = cleaned.replace("’", "")
    cleaned = cleaned.replace("\\n", "")
    # cleaned = _cleaner_text(str(textIn))
    cleaned = re.sub("b'", ' ', cleaned)
    regex = re.compile(r'[^a-zA-Z0-9 ]')
    cleaned = regex.sub(" ", cleaned)

    # cleaned = re.sub(' +', ' ', cleaned)
    cleaned = re.sub('\.+', '.', cleaned)

    return cleaned.lower().strip()


def corpus_as_sentence_list(package: data_models.PipelinePackage):
    log.getLogger().warning("#########################################")
    log.getLogger().warning("Converting entire corpus into a list of sentences")
    log.getLogger().warning("This is going to take forever and a day")
    log.getLogger().warning("Go make a cup of tea")
    sentence_list = []
    for doc in package.linked_document_list:
        linked_doc_by_sentence_list = split_linked_doc_by_sentence(doc)
        for doc_by_sentence in linked_doc_by_sentence_list:
            sentence_list.append(doc_by_sentence.raw)
            # log.getLogger().info(doc_by_sentence.raw)
    return sentence_list


def corpus_as_tokenized_sentence_linked_doc_list(package: data_models.PipelinePackage, clean_raw_sentences=False):
    log.getLogger().warning("#########################################")
    log.getLogger().warning("Converting entire corpus into a list of tokenized sentences")
    log.getLogger().warning("This is going to take forever and a day")
    log.getLogger().warning("Go make a cup of tea")
    sentence_list = []
    for doc in package.linked_document_list:
        linked_doc_by_sentence_list = split_linked_doc_by_sentence(doc)
        for doc_by_sentence in linked_doc_by_sentence_list:
            doc_by_sentence.tokens = clean_string_for_tokenizing(doc_by_sentence.raw.lower()).split()
            if clean_raw_sentences == True:
                doc_by_sentence.raw = clean_raw_content(doc_by_sentence.raw)

            sentence_list.append(doc_by_sentence)
            # log.getLogger().info(doc_by_sentence.tokens)

    return sentence_list


def sentence_to_linked_doc(sentence,
                           title="",
                           tokens=[],
                           src="",
                           ui="",
                           provider="",
                           uid="",
                           index_name="",
                           space="",
                           scores="",
                           corpus_doc=[],
                           any_analysis={},
                           any_inputs={},
                           updated=0,
                           groupedBy=""
                           ):
    linked_doc = merm_model.LinkedDocument(
        raw = sentence,
        title = title ,
        tokens =tokens,
        src = src,
        ui = ui,
        provider = provider,
        uid = uid,
        index_name = index_name,
        space =  space,
        scores = scores,
        corpus_doc = corpus_doc,
        any_analysis = any_analysis,
        any_inputs = any_inputs,
        updated = updated,
        groupedBy = groupedBy)
    linked_doc = tokenize_one_linked_doc(linked_doc)
    return linked_doc


def corpus_as_tokenized_sentence_linked_doc_list_grouped_by_doc(package: data_models.PipelinePackage,
                                                                    clean_raw_sentences=False):

    log.getLogger().warning("#########################################")
    log.getLogger().warning("Converting entire corpus into a list of tokenized sentences")
    log.getLogger().warning("This is going to take forever and a day")
    log.getLogger().warning("Go make a cup of tea")

    doc_dict = {}
    for doc in package.linked_document_list:
        sentence_list = []
        linked_doc_by_sentence_list = split_linked_doc_by_sentence(doc)
        for doc_by_sentence in linked_doc_by_sentence_list:
            doc_by_sentence.tokens = clean_string_for_tokenizing(doc_by_sentence.raw.lower()).split()
            if clean_raw_sentences == True:
                doc_by_sentence.raw = clean_raw_content(doc_by_sentence.raw)

            sentence_list.append(doc_by_sentence)
            #log.getLogger().info(doc_by_sentence.tokens)
        doc_dict[doc.uid] = sentence_list
    return doc_dict


def gensim_clean_string(textIn, _strip_tags=True, _split_alphanumeric=True, _strip_nonalphanumeric=True,
                        _strip_muliple_whitespace=True, _strip_short=True, _short_charcount_min=3,
                        _strip_punctuation=False, _convert_to_lower = False):
    cleaner = textIn
    if _strip_tags:
        cleaner = strip_tags(textIn)
    if _strip_nonalphanumeric:
        cleaner = strip_non_alphanum(cleaner)
    if _strip_muliple_whitespace:
        cleaner = strip_multiple_whitespaces(cleaner)
    if _split_alphanumeric:
        cleaner = split_alphanum(cleaner)
    if _strip_short:
        cleaner = strip_short(cleaner, minsize=_short_charcount_min)
    if _convert_to_lower:
        cleaner = cleaner.lower()


    return cleaner


def _stop_word_list_generator(package:merm_model.PipelinePackage):
    stop_list_path = package.dependencies_dict["env"].config["job_instructions"]["stop_list"]
    stop_list_string = package.dependencies_dict["env"].read_file(stop_list_path)
    log.getLogger().info("Stop words from file system")

    if _charFrequency(stop_list_string, ",") > 5:
        log.getLogger().info("comma delineated")
        stop_words = stop_list_string.split(",")
    else:
        log.getLogger().info("\\n delineated")
        stop_words = stop_list_string.split("\n")
    return stop_words

def _charFrequency( text, char):
    all_freq = {}
    for i in text:
        if i in all_freq:
            all_freq[i] += 1
        else:
            all_freq[i] = 1
    if char in all_freq.keys():
        return all_freq[char]
    else:
        return 0

def strip_stop_phrases(sentence, phrases):

    for phrase in phrases:
        if phrase.lower() in sentence.lower():
            return ""
    return sentence



def clean_raw_content(textIn):
    cleaner = textIn.replace("\\n", "")
    cleaner = strip_tags(cleaner)
    cleaner = strip_multiple_whitespaces(cleaner)
    cleaner = cleaner.lower()
    return cleaner


def cleanstring_simple(s):
    clean = s.replace('"', '').replace("\\n", "").replace(":", ";").replace("\\", " ").replace("..", ".")
    return clean

def cleanstring_doubled_period(s):
    clean = s.replace("..", ".").replace(". .",".").replace(" .", ".")
    return clean


def tokenize(corpora_list: List[merm_model.LinkedDocument]):
    for linked_doc in corpora_list:
        linked_doc.tokens = clean_string_for_tokenizing(linked_doc.raw.lower()).split()

    return corpora_list


def tokenize_one_linked_doc(linked_doc: merm_model.LinkedDocument):
    linked_doc.tokens = clean_string_for_tokenizing(str(linked_doc.raw).lower()).split(" ")
    return linked_doc


def split_linked_doc_by_sentence(linked_doc: merm_model.LinkedDocument):
    linked_sentence_list = []
    raw_sentences = str(linked_doc.raw).split(".")
    for sentence in raw_sentences:
        linked_sentence = merm_model.LinkedDocument(sentence, linked_doc.title, [], linked_doc.source, linked_doc.ui,
                                                    linked_doc.provider, linked_doc.uid, linked_doc.index_name,
                                                    linked_doc.space, linked_doc.scores, linked_doc.corpus_doc,
                                                    linked_doc.any_analysis, linked_doc.any_inputs, linked_doc.updated,
                                                    linked_doc.groupedBy)
        linked_sentence = tokenize_one_linked_doc(linked_sentence)
        linked_sentence_list.append(linked_sentence)
    return linked_sentence_list


def hasNumbers(inputString):
    return bool(re.search(r'\d', inputString))


def standard_stop_words():
    return ["i", "me", "my", "myself", "we", "our", "ours", "ourselves", "you", "your", "yours", "yourself",
            "yourselves",
            "he", "him", "his", "himself", "she", "her", "hers", "herself", "it", "its", "itself", "they", "them",
            "their",
            "theirs", "themselves", "what", "which", "who", "whom", "this", "that", "these", "those", "am", "is",
            "are",
            "was", "were", "be", "been", "being", "have", "has", "had", "having", "do", "does", "did", "doing", "a",
            "an",
            "the", "and", "but", "if", "or", "because", "as", "until", "while", "of", "at", "by", "for", "with",
            "about",
            "against", "between", "into", "through", "during", "before", "after", "above", "below", "to", "from",
            "up",
            "down", "in", "out", "on", "off", "over", "under", "again", "further", "then", "once", "here", "there",
            "when",
            "where", "why", "how", "all", "any", "both", "each", "few", "more", "most", "other", "some", "such",
            "no",
            "nor", "not", "only", "own", "same", "so", "than", "too", "very", "s", "t", "can", "will", "just",
            "don",
            "should", "now", "n", "html", "p", "div", "li", "val", "def", "id", "quot", "http", "com", "merm", "data",
            "file",
            "skin", "tone", "slightly", "smiling", "face", "thumbs", "open", "rescoped", "opened", "commented",
            "updated", "rescope",
            "open", "comment", "update"]
