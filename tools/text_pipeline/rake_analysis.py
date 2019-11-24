import tools.model.model_classes as merm_model
import tools.utils.log as log
import tools.utils.envutils as env

import RAKE.RAKE as rake
import operator

class RakeAnalysis:

    def __init__(self):
        pass

    def perform(self, package: merm_model.PipelinePackage):
        keywords_by_document_dict = {}
        df = package.corpus
        text_field = package.dependencies_dict["env"].config["ml_instructions"]["rake_textfield_in_df"]
        stop_words = package.dependencies_dict["env"].config["job_instructions"]["stop_list"]
        log.getLogger().info("Shape of DF: " + str(df.shape))

        for index, row in df.iterrows():
            document = row[text_field]
            resulting_keywords = self._rake_analysis(document, stop_words)
            keywords_by_document_dict[row["id"]] = (resulting_keywords, row["majorFinal"])

        package.any_analysis_dict["rake"] = keywords_by_document_dict
        return package

    def _rake_analysis(self, document, stop_words):
        rake_object = rake.Rake(stop_words)
        keywords = rake_object.run(document)
        return keywords




