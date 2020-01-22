import tools.model.model_classes as merm_model

class EsExtract:

    def __init__(self):
        pass

    def perform(self,package:merm_model.PipelinePackage):
        es_extract = package.dependencies_dict["es_extract"]
        es_conn = package.dependencies_dict["es_conn"]

        return_package:merm_model.PipelinePackage = es_extract.initiate_extraction(es_conn, package)
        return_package.log_stage("Returning a dataframe " + str(return_package.corpus.shape))
        return return_package

