
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
        groupby_count = {}
        numeric_class = []
        corpus = []
        category_count = 0
        for linked_doc in package.linked_document_list:
            corpus.append(linked_doc.raw)
            category = linked_doc.space
            if category in classes.keys():
                numeric_class.append(classes[category])
                groupby_count[category] = groupby_count[category] + 1
            else:
                classes[category] = category_count
                numeric_class.append(category_count)
                category_count = category_count + 1
                groupby_count[category] =  1



        package.any_analysis_dict["scikit_category_catalog"] = classes

        env = package.dependencies_dict["env"]
        vectorizer_type = env.config["ml_instructions"]["vectorizer_type"]
        if "tfidf" in vectorizer_type.lower():
            vectorizer = TfidfVectorizer(analyzer='word', token_pattern=r'\w{1,}', max_features=5000)
        else:
            vectorizer = CountVectorizer(analyzer='word', ngram_range=(1, 1), min_df=0, stop_words='english')
        matrix = vectorizer.fit_transform(corpus)
        feature_names = vectorizer.get_feature_names()

        package.any_inputs_dict["RFX"] = matrix
        package.any_inputs_dict["RFY"] = numeric_class
        package.any_inputs_dict["RFdict"] = feature_names
        package.any_inputs_dict["RFcategories"] = classes


        class_string = "\n"
        for key, count in classes.items():
            class_string = class_string + key +"\t" + str(count) + "\n"

        groupby_string = "\n"
        for group, cnt in groupby_count.items():
            groupby_string = groupby_string + group + "\t" + str(cnt) + "\n"

        package.log_stage("\nPrepared corpus. \nVectorizor type:"+ vectorizer_type +"\nCategory map " +str(classes.keys())+"\n"+class_string +"\n\n"+groupby_string)
        new_package = merm_model.PipelinePackage(None, (numeric_class, matrix), feature_names,
                                                 package.linked_document_list, package.any_analysis_dict,
                                                 package.any_inputs_dict, package.dependencies_dict)
        return new_package