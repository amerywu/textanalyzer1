import tools.model.model_classes as merm_model
import tools.utils.log as log
import tools.utils.envutils as env
import statistics as stats

class MergeCategories1:

    def __init__(self):
        pass

    def perform(self,package:merm_model.PipelinePackage):
        for linked_doc in package.linked_document_list:
            category_split = linked_doc.groupedBy.split("_")
            excess = category_split.pop()
            category = ""
            for c in category_split:
                category = category + c
            linked_doc.groupedBy = category
        return package

