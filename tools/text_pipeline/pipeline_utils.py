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

class Loop:

    def __init__(self):
        pass

    def perform(self,package:merm_model.PipelinePackage):
        if "current_loop" in package.any_inputs_dict.keys():
            current_loop = package.any_inputs_dict["current_loop"]
        else:
            current_loop = 0
        env = package.dependencies_dict["env"]
        package.any_inputs_dict["current_loop"] = current_loop + 1
        loop_count = env.config.getint("pipeline_instructions", "loop_count")
        package.any_inputs_dict["loop_count"] = loop_count
        package.log_stage("Current loop: " + str(current_loop) + "\nTotal loops: " + str(loop_count) )
        return package


