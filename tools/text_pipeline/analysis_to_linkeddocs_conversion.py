import tools.model.model_classes as merm_model

def generate_linked_docs_ranked(package: merm_model.PipelinePackage, analysis_key):
    linked_doc_dict = {}
    package.cache_linked_docs()
    all_groups = package.any_analysis_dict[analysis_key]
    for key, item_dict in all_groups.items():
        linked_doc_list = []
        for rank, item_list in item_dict.items():
            for sentence in item_list:
                linked_doc = package.dependencies_dict["utils"].sentence_to_linked_doc(sentence)
                linked_doc_list.append(linked_doc)
            linked_doc_dict[key + "_" + str(rank)] = linked_doc_list
    package.linked_document_list = linked_doc_dict
    return package

def generate_linked_docs_unranked(package: merm_model.PipelinePackage, analysis_key):
    linked_doc_dict = {}
    package.cache_linked_docs()
    all_groups = package.any_analysis_dict[analysis_key]
    for key, sentence_list in all_groups.items():
        linked_doc_list = []

        for sentence in sentence_list:
            linked_doc = package.dependencies_dict["utils"].sentence_to_linked_doc(sentence)
            linked_doc_list.append(linked_doc)
        linked_doc_dict[key] = linked_doc_list
    package.linked_document_list = linked_doc_dict
    return package

class TextRankResultsToLinkedDocList:

    def __init__(self):
        pass

    def perform(self, package: merm_model.PipelinePackage):
        return generate_linked_docs_ranked(package, "text_rank_all_groups")


class RakeResultsToLinkedDocList:

    def __init__(self):
        pass

    def perform(self, package: merm_model.PipelinePackage):
        return generate_linked_docs_ranked(package, "rake_all_groups")

class NounPhraseResultsToLinkedDocList:

    def __init__(self):
        pass

    def perform(self, package: merm_model.PipelinePackage):
        return generate_linked_docs_ranked(package, "noun_phrase_all_groups")

