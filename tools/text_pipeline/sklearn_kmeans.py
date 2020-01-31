
import tools.model.model_classes as merm_model
from sklearn.cluster import AgglomerativeClustering
from datetime import datetime
import tools.utils.log as log
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_samples, silhouette_score
import numpy as np
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
        id =  "km_" + str(rf_count) + "_" + str(category_count)  + "_" + str(len(package.any_inputs_dict["SKY"])) + "_" + suffix
        return id

    def _silhouette(self, X):

        cluster_count = [10, 12,14,16,18,20,22,24,26, 28, 30, 35, 40, 45, 50, 55, 60]
        results = []
        for count in cluster_count:
            clusterer = KMeans(n_clusters=count, random_state=10)
            cluster_labels = clusterer.fit_predict(X)

            # The silhouette_score gives the average value for all the samples.
            # This gives a perspective into the density and separation of the formed
            # clusters
            silhouette_avg = silhouette_score(X, cluster_labels)
            results.append([count, silhouette_avg])
        return results

    def perform(self, package: merm_model.PipelinePackage):
        analysis_id = self._analysis_id(package)
        log.getLogger().info("K means prediciting. Tea time")
        X = package.any_inputs_dict["SKX"]

        Xarray = X.toarray()
        silhouette_results = self._silhouette(Xarray)
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


