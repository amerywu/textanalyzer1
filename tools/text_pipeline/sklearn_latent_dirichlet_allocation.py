from sklearn.model_selection import GridSearchCV
from sklearn.decomposition import NMF, LatentDirichletAllocation
import tools.utils.log as log
import tools.model.model_classes as merm_model




class ScikitLDA:

    def __init__(self):
        pass

    def perform(self, package: merm_model.PipelinePackage):
        new_model = self._doLDA(package.corpus)
        new_package = merm_model.PipelinePackage(new_model,package.corpus,package.dict, package.linked_document_list,package.any_analysis_dict, package.dependencies_dict)
        log.getLogger().info(new_package.structure())
        return new_package

    def _doLDA(self,
               X,
               components=10,
               maxiter=20,
               learningmethod='online',
               learningoffset=0,
               randomstate=10,
               verbose=1):
        model = LatentDirichletAllocation(n_components=components,
                                          max_iter=maxiter,
                                          learning_method=learningmethod,
                                          learning_offset=learningoffset,
                                          random_state=randomstate,
                                          verbose=verbose).fit(X)

        return model

class SciKitLDAReport:
    def __init__(self):
        pass

    def perform(self, package: merm_model.PipelinePackage):
        self.filterAndReportResultsLDA(package.model,package.dict)

    def filterAndReportResultsLDA(self, model, dict, n_top_words=10):
        for topic_idx, topic in enumerate(model.components_):
            print("Topic %d:" % (topic_idx))
            words = []
            for i in topic.argsort()[:-n_top_words - 1:-1]:
                words.append(dict[i])
            print(words)