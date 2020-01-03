import tools.model.model_classes as merm_model
import tools.utils.log as log
import tools.utils.envutils as env
import sys
class DfGroupByAnalysis:

    def __init__(self):
        pass

    def perform(self, package: merm_model.PipelinePackage):

        df = package.corpus
        log.getLogger().info("Shape of DF: " + str(df.shape))
        groupby_dict = {}
        column = package.dependencies_dict["env"].config["ml_instructions"]["df_groupby_column"]


        count = 0
        for index, row in df.iterrows():
            count = count + 1
            if count % 1000 == 0:
                sys.stdout.write(".")
            jobs_string = row[column]

            if jobs_string in groupby_dict.keys():
                groupby_dict[jobs_string] = groupby_dict[jobs_string] + 1
            else:
                groupby_dict[jobs_string] = 1
        package.log_stage("Broke a pandas data frame into a dict of data grouped by " + str(column))
        package.any_analysis_dict["group_by_"+column] = groupby_dict

        return package



