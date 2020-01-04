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
        stop_words_path = package.dependencies_dict["env"].config["job_instructions"]["stop_list"]


        log.getLogger().info("Shape of DF: " + str(df.shape))

        for index, row in df.iterrows():
            document = row[text_field]
            resulting_keywords = self._rake_analysis(document, stop_words_path)
            keywords_by_document_dict[row["id"]] = (resulting_keywords, row["majorFinal"])

        package.any_analysis_dict["rake"] = keywords_by_document_dict
        return package

    def _rake_analysis(self, document, stop_words):
        rake_object = rake.Rake(stop_words)
        keywords = rake_object.run(document)
        return keywords




class RakeAnalysisLinkedDocList:

    def __init__(self):
        pass

    def perform(self, package: merm_model.PipelinePackage):
        keywords_by_document_dict = {}

        stop_words = package.dependencies_dict["env"].config["job_instructions"]["stop_list"]


        for doc in package.linked_document_list:

            resulting_keywords = self._rake_analysis(doc.raw, stop_words)
            keywords_by_document_dict[doc.groupedBy] = (resulting_keywords, doc.groupedBy)

        package.any_analysis_dict["rake"] = keywords_by_document_dict
        return package

    def _rake_analysis(self, document, stop_words):
        rake_object = rake.Rake(stop_words)
        keywords = rake_object.run(document)
        return keywords


class RakeAnalysisFromTextRank:

    def __init__(self):
        pass

    def perform(self, package: merm_model.PipelinePackage):

        keywords_by_document_dict = {}
        stop_words_path = package.dependencies_dict["env"].config["job_instructions"]["stop_list"]
        colutils = package.dependencies_dict["colutils"]

        analysis_key = colutils.get_top_incrementing_key("text_rank", package.any_analysis_dict)
        text_dict = package.any_analysis_dict[analysis_key]

        for idx, summary_level in text_dict.items():
            text = '. '.join(summary_level)
            resulting_keywords = self._rake_analysis(text, stop_words_path)
            keywords_by_document_dict[idx] = resulting_keywords
        analysis_key = colutils.incrementing_key("rake", package.any_analysis_dict)
        package.any_analysis_dict[analysis_key] = keywords_by_document_dict

        package.log_stage("Performed rake analysis on the text rank results. The stop word list was from " + stop_words_path)
        return package

    def _rake_analysis(self, document, stop_words):
        rake_object = rake.Rake(stop_words)
        keywords = rake_object.run(document)
        return keywords
