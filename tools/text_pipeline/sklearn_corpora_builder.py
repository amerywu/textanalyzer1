
import tools.utils.log as log
import tools.model.model_classes as merm_model
from sklearn.feature_extraction.text import TfidfVectorizer, CountVectorizer
from sklearn.decomposition import NMF


class LinkedDocListToScikitLDACorpus:

    def __init__(self):
        pass

    def perform(self, package: merm_model.PipelinePackage):
        corpus = []
        for linked_doc in package.linked_document_list:
            corpus.append(linked_doc.raw)

        vectorizer = CountVectorizer(analyzer='word', ngram_range=(1, 1), min_df=0, stop_words='english')
        matrix = vectorizer.fit_transform(corpus)
        feature_names = vectorizer.get_feature_names()

        new_package = merm_model.PipelinePackage(None,matrix,feature_names,
                                                 package.linked_document_list,package.any_analysis_dict,
                                                 package.any_inputs_dict, package.dependencies_dict)
        return new_package



class LinkedDocListToScikitRFCorpus:

    def __init__(self):
        pass

    def perform(self, package: merm_model.PipelinePackage):
        classes = {}
        numeric_class = []
        corpus = []
        category_count = 0
        for linked_doc in package.linked_document_list:
            corpus.append(linked_doc.raw)
            category = linked_doc.space
            if category in classes.keys():
                numeric_class.append(classes[category])
            else:
                classes[category] = category_count
                numeric_class.append(category_count)
                category_count = category_count + 1



        package.any_analysis_dict["scikit_category_catalog"] = classes
        vectorizer = CountVectorizer(analyzer='word', ngram_range=(1, 1), min_df=0, stop_words='english')
        matrix = vectorizer.fit_transform(corpus)
        feature_names = vectorizer.get_feature_names()

        new_package = merm_model.PipelinePackage(None,(numeric_class,matrix),feature_names,
                                                 package.linked_document_list,package.any_analysis_dict,
                                                 package.any_inputs_dict, package.dependencies_dict)
        return new_package