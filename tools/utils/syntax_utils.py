from nltk.corpus import wordnet as wn
import spacy
import tools.utils.text_parsing_utils as text_utils
from nltk.stem import WordNetLemmatizer
import tools.utils.envutils as env
import tools.utils.log as log
import tools.model.model_classes as data_models
import tools.model.model_classes as merm_model
from typing import List

def nounify(verb_word):
    """ Transform a verb to the closest noun: die -> death """
    verb_synsets = wn.synsets(verb_word, pos="v")

    # Word not found
    if not verb_synsets:
        return []

    # Get all verb lemmas of the word
    verb_lemmas = [l for s in verb_synsets \
                   for l in s.lemmas() if s.name().split('.')[1] == 'v']

    # Get related forms
    derivationally_related_forms = [(l, l.derivationally_related_forms()) \
                                    for l in    verb_lemmas]

    # filter only the nouns
    related_noun_lemmas = [l for drf in derivationally_related_forms \
                           for l in drf[1] if l.synset().name().split('.')[1] == 'n']

    # Extract the words from the lemmas
    words = [l.name for l in related_noun_lemmas]
    len_words = len(words)

    # Build the result in the form of a list containing tuples (word, probability)
    result = [(w, float(words.count(w))/len_words) for w in set(words)]
    result.sort(key=lambda w: -w[1])

    # return all the possibilities sorted by probability
    return result



def verbify(noun_word):
    """ Transform a verb to the closest noun: die -> death """
    noun_synsets = wn.synsets(noun_word, pos="n")

    # Word not found
    if not noun_synsets:
        return []

    # Get all verb lemmas of the word
    noun_lemmas = [l for s in noun_synsets \
                   for l in s.lemmas() if s.name().split('.')[1] == 'n']

    # Get related forms
    derivationally_related_forms = [(l, l.derivationally_related_forms()) \
                                    for l in    noun_lemmas]

    # filter only the nouns
    related_verb_lemmas = [l for drf in derivationally_related_forms \
                           for l in drf[1] if l.synset().name().split('.')[1] == 'v']

    # Extract the words from the lemmas
    words = [l.name for l in related_verb_lemmas]
    len_words = len(words)

    # Build the result in the form of a list containing tuples (word, probability)
    result = [(w, float(words.count(w))/len_words) for w in set(words)]
    result.sort(key=lambda w: -w[1])

    # return all the possibilities sorted by probability
    return result


def lemmatize_tokens(corpora_list: List[merm_model.LinkedDocument], stop_words: List[str]):
    nlp = spacy.load('en_core_web_sm')
    stoplist = stop_words
    lemmatized_corpus = []
    iter_count = 0
    lemmatizer = WordNetLemmatizer()
    # log.getLogger().info("Lemmatizing corpus. This can be slow.")
    for doc in corpora_list:
        lemmatized_text = []
        for word in doc.tokens:
            # print("word: " + word)
            lemmatized_word = lemmatizer.lemmatize(word)
            if lemmatized_word is not None:
                cleanword = text_utils.clean_string_for_tokenizing(lemmatized_word)
                if cleanword not in stoplist and len(cleanword) > 1 and not text_utils.hasNumbers(cleanword):
                    # print(cleanword)
                    lemmatized_text.append(cleanword)
        doc.tokens = lemmatized_text
        lemmatized_corpus.append(doc)
        iter_count += 1

        if env.test_env() == True and iter_count > env.test_env_doc_processing_count():
            log.getLogger().info("DEV MODE: Breaking loop here")
            break
    return lemmatized_corpus


def is_verb(tag):
    return tag in ['VB', 'VBD', 'VBG', 'VBN', 'VBP', 'VBZ']


def lemmatize_docs(corpora_list: List[merm_model.LinkedDocument], stop_words: List[str]):
    nlp = spacy.load('en_core_web_sm')
    stoplist = stop_words
    lemmatized_corpus = []
    iter_count = 0
    lemmatizer = WordNetLemmatizer()
    log.getLogger().info("Lemmatizing corpus. This can be slow.")

    for doc in corpora_list:
        lemmatized_text = []
        print("sentence")


        for token in doc.tokens:
            lemmatized_word = lemmatizer.lemmatize(token)

            if lemmatized_word is not None:
                cleanword = text_utils.clean_string_for_tokenizing(lemmatized_word)
                if cleanword not in stoplist and len(cleanword) > 1 and not text_utils.hasNumbers(cleanword):
                    # print(cleanword)
                    lemmatized_text.append(cleanword)
        doc.tokens = lemmatized_text
        lemmatized_corpus.append(doc)
        iter_count += 1

        if env.test_env() == True and iter_count > env.test_env_doc_processing_count():
            log.getLogger().info("DEV MODE: Breaking loop here")
            break
    return lemmatized_corpus


def lemmatize_docs_properly(corpora_list: List[merm_model.LinkedDocument], stop_words: List[str]):
    nlp = spacy.load('en_core_web_sm')
    stoplist = stop_words
    lemmatized_corpus = []
    iter_count = 0
    lemmatizer = WordNetLemmatizer()
    log.getLogger().info("Lemmatizing corpus. This can be slow.")

    for doc in corpora_list:
        lemmatized_text = []
        print("sentence")
        nlp_doc = nlp(doc.raw)

        for token in nlp_doc:
            tag = token.tag_
            text = token.text
            lemmatized_word =""
            if is_verb(tag):
                lemmatized_word = lemmatizer.lemmatize(text, "v")
            else:
                lemmatized_word = lemmatizer.lemmatize(text)

            if lemmatized_word is not None:
                cleanword = text_utils.clean_string_for_tokenizing(lemmatized_word)
                if cleanword not in stoplist and len(cleanword) > 1 and not text_utils.hasNumbers(cleanword):
                    # print(cleanword)
                    lemmatized_text.append(cleanword)
        doc.tokens = lemmatized_text
        lemmatized_corpus.append(doc)
        iter_count += 1

        if env.test_env() == True and iter_count > env.test_env_doc_processing_count():
            log.getLogger().info("DEV MODE: Breaking loop here")
            break
    return lemmatized_corpus
