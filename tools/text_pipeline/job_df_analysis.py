import tools.model.model_classes as merm_model
import tools.utils.log as log
import tools.utils.envutils as env

class JobDfAnalysis:

    def __init__(self):
        pass

    def perform(self, package: merm_model.PipelinePackage):
        if ("job" not in package.any_analysis_dict["provider"]):
            raise Exception("This classs will not work on " + str(package.any_analysis_dict["provider"]))
        df = package.corpus
        log.getLogger().info("Shape of DF: " + str(df.shape))
        jobs_dict = {}

        for index, row in df.iterrows():
            majorFinal = row["majorFinal"]
            if majorFinal is None:
                jobs_string = row["jobFinal"]

                if jobs_string in jobs_dict.keys():
                    jobs_dict[jobs_string] = jobs_dict[jobs_string] + 1
                else:
                    jobs_dict[jobs_string] = 1
        package.any_analysis_dict["no_major_jobs_count"] = jobs_dict

        for index, row in df.iterrows():
            jobs_string = row["jobFinal"]

            if jobs_string in jobs_dict.keys():
                jobs_dict[jobs_string] = jobs_dict[jobs_string] + 1
            else:
                jobs_dict[jobs_string] = 1
        package.any_analysis_dict["jobs_count"] = jobs_dict

        return package


class AreasOfStudyDfAnalysis:

    def __init__(self):
        pass

    def perform(self,package:merm_model.PipelinePackage):
        if("job" not in package.any_analysis_dict["provider"]):
            raise Exception("This classs will not work on " + str(package.any_analysis_dict["provider"]))
        df = package.corpus
        log.getLogger().info("Shape of DF: " + str(df.shape))
        areas_of_study_dict_undefined = {}

        for index, row in df.iterrows():
            majorFinal = row["majorFinal"]
            if majorFinal is None:
                areas_of_study = row["areasOfStudy"]
                if len(areas_of_study) > 0:
                    areasOfStudyList = areas_of_study.split(",")
                    for s in areasOfStudyList:
                        if s in areas_of_study_dict_undefined.keys():
                            areas_of_study_dict_undefined[s] = areas_of_study_dict_undefined[s] + 1
                        else:
                            areas_of_study_dict_undefined[s] = 1
        package.any_analysis_dict["undefined_areas_of_study_count"] = areas_of_study_dict_undefined

        areas_of_study_dict = {}
        for index, row in df.iterrows():
            majorFinal = row["majorFinal"]

            if majorFinal in areas_of_study_dict.keys():
                areas_of_study_dict[majorFinal] = areas_of_study_dict[majorFinal] + 1
            else:
                areas_of_study_dict[majorFinal] = 1
        package.any_analysis_dict["areas_of_study_count"] = areas_of_study_dict

        return package






