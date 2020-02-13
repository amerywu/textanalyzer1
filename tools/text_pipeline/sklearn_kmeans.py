
import tools.model.model_classes as merm_model
from tools.text_pipeline.text_cleaner import TokensToDoc
from tools.text_pipeline.sklearn_corpora_builder import LinkedDocListToScikitRFCorpus
from sklearn.cluster import AgglomerativeClustering
from datetime import datetime
import tools.utils.log as log

from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_samples, silhouette_score
import numpy as np


def _silhouette( X,cluster_count ):

    results = []
    for i in range(0, len(cluster_count)):
        cluster_count[i] = int(cluster_count[i])
    for count in cluster_count:
        log.getLogger().info("Silhouette Testing for " + str(count) + " clusters.")
        clusterer = KMeans(n_clusters=count, random_state=10)
        cluster_labels = clusterer.fit_predict(X)

        # The silhouette_score gives the average value for all the samples.
        # This gives a perspective into the density and separation of the formed
        # clusters
        silhouette_avg = silhouette_score(X, cluster_labels)
        results.append([count, silhouette_avg])
    return results

class ScikitAgglomerativeKmeans:

    def __init__(self):
        pass

    def _analysis_id(self, package: merm_model.PipelinePackage):
        dt = datetime.now()
        suffix = str(dt.microsecond)[-4:]
        if "kmeans_iteration_count" in package.any_inputs_dict.keys():
            rf_count = package.any_inputs_dict["kmeans_iteration_count"] + 1
            package.any_inputs_dict["kmeans_iteration_count"] = rf_count
        else:
            rf_count = 0
            package.any_inputs_dict["kmeans_iteration_count"] = rf_count

        categories = package.any_inputs_dict["SKcategories"]
        category_count = len(list(categories.keys()))
        id =  "kma_" + str(rf_count) + "_" + str(category_count)  + "_" + str(len(package.any_inputs_dict["SKY"])) + "_" + suffix
        return id

    def perform(self, package: merm_model.PipelinePackage):
        analysis_id = self._analysis_id(package)
        log.getLogger().info("K means prediciting. Tea time")
        X = package.any_inputs_dict["SKX"]
        env = package.dependencies_dict["env"]
        test_range = env.config["ml_instructions"] ["silhouette_range"].split(",")

        Xarray = X.toarray()
        silhouette_results = _silhouette(Xarray,test_range)
        cluster_count_tuple = max(silhouette_results, key=lambda x:x[1])
        y = package.any_inputs_dict["SKY"]
        skdict = package.any_inputs_dict["SKdict"]
        cluster = AgglomerativeClustering(n_clusters=cluster_count_tuple[0], affinity='euclidean', linkage='ward')

        result = cluster.fit_predict(X.toarray())
        labels = cluster.labels_
        cluster_list = []
        for j in range(labels.shape[0]):
            row_list = []
            sentence = package.linked_document_list[j].raw
            cluster = labels[j]
            row_list.append(cluster)
            row_list.append(sentence)
            cluster_list.append(row_list)
        cluster_list

        package.any_analysis_dict[analysis_id+"_result"] = cluster_list
        package.log_stage("Agglomerative Clustering\nSilhouette : " + str(silhouette_results) + "\nCluster count : " + str(cluster_count_tuple))
        return package

class ScikitKmeansNoRepeats:
    def __init__(self):
        pass

    def _analysis_id(self, package: merm_model.PipelinePackage):
        dt = datetime.now()
        suffix = str(dt.microsecond)[-4:]
        if "kmeans_iteration_count" in package.any_inputs_dict.keys():
            rf_count = package.any_inputs_dict["kmeans_iteration_count"] + 1
            package.any_inputs_dict["kmeans_iteration_count"] = rf_count
        else:
            rf_count = 0
            package.any_inputs_dict["kmeans_iteration_count"] = rf_count

        categories = package.any_inputs_dict["SKcategories"]
        category_count = len(list(categories.keys()))
        id =  "km1_" + str(rf_count) + "_" + str(category_count)  + "_" + str(len(package.any_inputs_dict["SKY"])) + "_" + suffix
        return id

    def perform(self, package: merm_model.PipelinePackage):
        analysis_id = self._analysis_id(package)
        log.getLogger().info("K means prediciting. Tea time")
        X = package.any_inputs_dict["SKX"]
        env = package.dependencies_dict["env"]
        test_range = env.config["ml_instructions"] ["silhouette_range"].split(",")
        reporting_count = env.config.getint("ml_instructions","sklearn_kmeans_term_per_cluster_reporting_count")

        Xarray = X.toarray()
        silhouette_results = _silhouette(Xarray,test_range)
        cluster_count_tuple = max(silhouette_results, key=lambda x:x[1])

        skdict = package.any_inputs_dict["SKdict"]
        kmeans = KMeans(n_clusters=cluster_count_tuple[0], random_state=10)
        kmeans.fit_predict(Xarray)

        centers = kmeans.cluster_centers_.argsort()[:, ::-1]

        centroid_list = []
        centroid_list.append(["cluster","1","2","3","4","5","6","7","8","9","10","11","12","13","14","15","16"])
        for i in range(cluster_count_tuple[0]):
            row_list = [i]
            for ind in centers[i, :reporting_count]:
                row_list.append(skdict[ind])

            centroid_list.append(row_list)


        cluster_list = []
        cluster_list.append(["cluster","sentence"])

        package.any_analysis_dict[analysis_id + "_top_terms"] = centroid_list
        package.any_inputs_dict["kmeans_top_terms_key"] = analysis_id + "_top_terms"
        package.log_stage("Kmeans Clustering, no repeats\nSilhouette : " + str(silhouette_results) + "\nCluster count : " + str(cluster_count_tuple))
        return package


class ScikitKmeansWithTermRemoval:
    def __init__(self):
        pass

    def _analysis_id(self, package: merm_model.PipelinePackage):
        dt = datetime.now()
        suffix = str(dt.microsecond)[-4:]
        if "kmeans_iteration_count" in package.any_inputs_dict.keys():
            rf_count = package.any_inputs_dict["kmeans_iteration_count"] + 1
            package.any_inputs_dict["kmeans_iteration_count"] = rf_count
        else:
            rf_count = 0
            package.any_inputs_dict["kmeans_iteration_count"] = rf_count

        categories = package.any_inputs_dict["SKcategories"]
        category_count = len(list(categories.keys()))
        id = "km1_" + str(rf_count) + "_" + str(category_count) + "_" + str(
            len(package.any_inputs_dict["SKY"])) + "_" + suffix
        return id


    def _run_kmeans(self, package):
        log.getLogger().info("K means predicting. Tea time")
        X = package.any_inputs_dict["SKX"]
        env = package.dependencies_dict["env"]
        test_range = env.config["ml_instructions"]["silhouette_range"].split(",")


        Xarray = X.toarray()
        silhouette_results = _silhouette(Xarray, test_range)
        cluster_count_tuple = max(silhouette_results, key=lambda x: x[1])


        kmeans = KMeans(n_clusters=cluster_count_tuple[0], random_state=10)
        kmeans.fit(Xarray)
        return kmeans

    def perform(self, package: merm_model.PipelinePackage):
        analysis_id = self._analysis_id(package)
        env = package.dependencies_dict["env"]
        skdict = package.any_inputs_dict["SKdict"]
        reporting_count = env.config.getint("ml_instructions", "sklearn_kmeans_term_per_cluster_reporting_count")
        max_term_overlap = env.config.getint("ml_instructions",
                                             "sklearn_kmeans_permitted_term_overlap_across_topics")

        kmeans = self._run_kmeans(package)
        centers = kmeans.cluster_centers_.argsort()[:, ::-1]

        centroid_list = []

        for i in range(kmeans.n_clusters):
            row_list = [i]
            for ind in centers[i, :reporting_count]:
                row_list.append(skdict[ind])

            centroid_list.append(row_list)


        stop_words = self._cluster_overlap(centroid_list, max_term_overlap)
        if len(stop_words) > max_term_overlap:
            log.getLogger().info("Cluster term overlap count: " + str(len(stop_words)))
            log.getLogger().info(str(stop_words))
            self._remove_stop_words(stop_words, package)
            t2d = TokensToDoc()
            package = t2d.perform(package)
            corpora_builder = LinkedDocListToScikitRFCorpus()
            package = corpora_builder.perform(package)
            package = self.perform(package)


        cluster_list = []
        cluster_list.append(["cluster", "sentence"])

        package.any_analysis_dict[analysis_id + "_top_terms"] = centroid_list
        package.any_inputs_dict["kmeans_top_terms_key"] = analysis_id + "_top_terms"
        package.log_stage(
            "Cluster term overlap count: " + str(len(stop_words)) +"\n stop words " + str(stop_words) +"\nCluster count : " + str(
                kmeans.n_clusters))
        return package

    def _cluster_overlap(self, centroid_list, max_term_overlap):
        term_freq_dict = {}
        for term_list in centroid_list:
            for term in term_list:
                if term in term_freq_dict.keys():
                    term_freq_dict[term] = term_freq_dict[term] + 1
                else:
                    term_freq_dict[term] = 1

        new_stop_words = []
        for term, freq in term_freq_dict.items():
            if freq > max_term_overlap:
                new_stop_words.append(term)
        return new_stop_words


    def _remove_stop_words(self, stop_words, package:merm_model.PipelinePackage):
        for linked_doc in package.linked_document_list:
            for word in stop_words:
                if word in linked_doc.tokens:
                    linked_doc.tokens.remove(word)
            linked_doc.raw = " ".join(linked_doc.tokens)
        return package

