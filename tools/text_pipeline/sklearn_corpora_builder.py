
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

    def _get_category(self, env):
        rf_category = env.config["ml_instructions"]["rf_category"]
        return rf_category

    def perform(self, package: merm_model.PipelinePackage):
        classes = {}
        groupby_count = {}
        numeric_class = []
        corpus = []
        category_count = 0
        env = package.dependencies_dict["env"]
        category_field = self._get_category(env)

        for linked_doc in package.linked_document_list:
            corpus.append(linked_doc.raw)
            if category_field == "group_by":
                category = linked_doc.groupedBy
            else:
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


        vectorizer_type = env.config["ml_instructions"]["vectorizer_type"]
        max_features = env.config.getint("ml_instructions","rf_max_features")

        if "tfidf" in vectorizer_type.lower():
            vectorizer = TfidfVectorizer(analyzer='word', token_pattern=r'\w{1,}', stop_words='english', max_features=max_features)
        else:
            vectorizer = CountVectorizer(analyzer='word', ngram_range=(1, 1), min_df=0, stop_words='english', max_features=max_features)
        matrix = vectorizer.fit_transform(corpus)
        feature_names = vectorizer.get_feature_names()

        package.any_inputs_dict["RFX"] = matrix
        package.any_inputs_dict["RFY"] = numeric_class
        package.any_inputs_dict["RFdict"] = feature_names
        package.any_inputs_dict["RFcategories"] = classes




        package.log_stage("\nPrepared corpus. \nVectorizor type:"+ vectorizer_type +"\nCategory map " +self.class_log(classes)+"\n Groupby map" +"\n\n"+self.groupby_log(groupby_count))
        new_package = merm_model.PipelinePackage(None, (numeric_class, matrix), feature_names,
                                                 package.linked_document_list, package.any_analysis_dict,
                                                 package.any_inputs_dict, package.dependencies_dict)
        return new_package

    def class_log(self, classes):
        class_string = "\n"
        for key, count in classes.items():
            class_string = class_string + key + "\t" + str(count) + "\n"
        return class_string

    def groupby_log(self, groupby_count):

        groupby_string = "\n"
        for group, cnt in groupby_count.items():
            groupby_string = groupby_string + group + "\t" + str(cnt) + "\n"
        return groupby_string


