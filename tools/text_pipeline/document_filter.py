import tools.model.model_classes as merm_model
import tools.utils.log as log
import statistics as stats
from random import shuffle

class GroupByESIndex:

    def __init__(self):
        pass

    def perform(self, package: merm_model.PipelinePackage):
        thetype = type(package.linked_document_list)
        if thetype is dict:
            return package

        linked_doc_by_index = {}
        group_testenv = package.dependencies_dict["env"].config.getboolean("pipeline_instructions", "group_testenv")
        if True == group_testenv:
            relevant_groups_string = package.dependencies_dict["env"].config["pipeline_instructions"][
                "group_testenv_list"]
            relevant_groups_list = relevant_groups_string.split(",")
            linked_doc_by_index = self._group_test_groups(package, relevant_groups_list)
        else:
            linked_doc_by_index = self._group_all_groups(package)

        new_package = merm_model.PipelinePackage(package.model, package.corpus, package.dict, linked_doc_by_index,
                                                 package.any_analysis, package.any_inputs_dict,
                                                 package.dependencies_dict)

        new_package.log_stage(
            "Divided the entire corpus into groups. The groups created are " + str(linked_doc_by_index.keys()))
        return new_package

    def _group_all_groups(self, package):
        linked_doc_by_index = {}
        for linked_doc in package.linked_document_list:

            if linked_doc.groupedBy in linked_doc_by_index:
                linked_doc_by_index[linked_doc.groupedBy].append(linked_doc)
            else:
                groupby_list = []
                groupby_list.append(linked_doc)
                linked_doc_by_index[linked_doc.groupedBy] = groupby_list

        return linked_doc_by_index

    def _group_test_groups(self, package, relevant_groups_list):
        linked_doc_by_index = {}
        for linked_doc in package.linked_document_list:

            if linked_doc.groupedBy in relevant_groups_list:

                if linked_doc.groupedBy in linked_doc_by_index:
                    linked_doc_by_index[linked_doc.groupedBy].append(linked_doc)
                else:
                    groupby_list = []
                    groupby_list.append(linked_doc)
                    linked_doc_by_index[linked_doc.groupedBy] = groupby_list

        return linked_doc_by_index


class StopWordRemoval:

    def __init__(self):
        self.stop_words_key = "stop_words"
        pass

    def perform(self, package: merm_model.PipelinePackage):

        log.getLogger().info("StopWordRemoval")
        stop_words = []
        load_source = ""
        if self.stop_words_key in package.any_analysis_dict:
            log.getLogger().info("Stop words retrieved from package analyses.")
            log.getLogger().debug("It's a list")
            stop_words = package.any_analysis_dict[self.stop_words_key]
            load_source = " from previous pipeline process."

        else:

            stop_list_path = package.dependencies_dict["env"].config["job_instructions"]["stop_list"]
            stop_list_string = package.dependencies_dict["env"].read_file(stop_list_path)
            log.getLogger().info("Stop words from file system")

            if package.dependencies_dict["utils"]._charFrequency(stop_list_string, ",") > 5:
                log.getLogger().info("comma delineated")
                stop_words = stop_list_string.split(",")
            else:
                log.getLogger().info("\\n delineated")
                stop_words = stop_list_string.split("\n")
            load_source = "from " + stop_list_path

        for linked_doc in package.linked_document_list:
            new_tokens = []
            for word in linked_doc.tokens:
                if not word in stop_words:
                    # log.getLogger().debug("removing " + word)
                    new_tokens.append(word)
            linked_doc.tokens = new_tokens

        package.log_stage(
            "Removed all stop words from the corpus tokens (i.e., bag of words). The stop words were loaded " + load_source)
        return package


class ExcludeBySpace:

    def __init__(self):
        pass

    def perform(self, package: merm_model.PipelinePackage):
        thetype = type(package.linked_document_list)
        if thetype is dict:
            return package


        include_list = package.dependencies_dict["env"].config["job_instructions"]["filter_space_include"].split(",")
        exclude_list = package.dependencies_dict["env"].config["job_instructions"]["filter_space_exclude"].split(",")

        included = self.include_docs(include_list, package.linked_document_list)
        new_linked_doc_list = self.exclude_list(exclude_list,included)
        log_include_result = "\nAfter include filter, remaining docs: " + str(len(new_linked_doc_list)) +"\n"



        new_package = merm_model.PipelinePackage(package.model, package.corpus, package.dict, new_linked_doc_list,
                                                 package.any_analysis, package.any_inputs_dict,
                                                 package.dependencies_dict)
        new_package.any_analysis_dict["newlist"] = len(new_linked_doc_list)
        new_package.any_analysis_dict["doclist"] = len(new_package.linked_document_list)
        new_package.log_stage(
            "\nInclude filter was: " + str(include_list) + "\nExclude filter was:" + str(
                exclude_list) + log_include_result + "\nRemaining documents count: " + str(
                len(new_linked_doc_list)))
        return new_package

    def include_docs(self, include_list, old_list):
        new_linked_doc_list = []

        if len(include_list) == 1 and len(include_list[0]) == 0:
            return old_list
        else:
            for linked_doc in old_list:
                for include in include_list:
                    if include:
                        if include in str(linked_doc.space):
                            new_linked_doc_list.append(linked_doc)
            return new_linked_doc_list

    def exclude_list(self, exclude_list, old_list):
        if len(exclude_list) == 1 and len(exclude_list[0]) == 0:
            return old_list
        new_list = []
        for linked_doc in old_list:
            keep = True
            for exclude in exclude_list:
                if exclude and  exclude in str(linked_doc.space):
                    # print("excluding "+ linked_doc.space)
                    keep = False
            if keep == True:
                new_list.append(linked_doc)
        return new_list




class ExcludeByGroup:

    def __init__(self):
        pass

    def perform(self, package: merm_model.PipelinePackage):
        thetype = type(package.linked_document_list)
        if thetype is dict:
            return package

        include_list = package.dependencies_dict["env"].config["job_instructions"]["filter_group_include"].split(",")
        exclude_list = package.dependencies_dict["env"].config["job_instructions"]["filter_group_exclude"].split(",")
        included = self.include_docs(include_list, package.linked_document_list)
        new_linked_doc_list = self.exclude_list(exclude_list,included)


        new_package = merm_model.PipelinePackage(package.model,
                                                 package.corpus,
                                                 package.dict,
                                                 new_linked_doc_list,
                                                 package.any_analysis,
                                                 package.any_inputs_dict,
                                                 package.dependencies_dict)

        new_package.log_stage(
            "\nInclude filter was: " + str(include_list) + "\nExclude filter was:" + str(exclude_list) + "Remaining documents count: " + str(
                len(new_linked_doc_list)))
        return new_package

    def include_docs(self, include_list, old_list):
        new_linked_doc_list = []

        if len(include_list) == 1 and len(include_list[0]) == 0:
            return old_list
        else:
            for linked_doc in old_list:
                for include in include_list:
                    if include:
                        if include in str(linked_doc.groupedBy):
                            new_linked_doc_list.append(linked_doc)
            return new_linked_doc_list

    def exclude_list(self, exclude_list, old_list):
        if len(exclude_list) == 1 and len(exclude_list[0]) == 0:
            return old_list
        new_list = []
        for linked_doc in old_list:
            keep = True
            for exclude in exclude_list:
                if exclude and exclude in str(linked_doc.groupedBy):
                    # print("excluding "+ linked_doc.space)
                    keep = False
            if keep == True:
                new_list.append(linked_doc)
        return new_list


class CountBySpaceAndGroup:
    def __init__(self):
        pass

    def perform(self, package: merm_model.PipelinePackage):
        classes = {}
        groupby_count = {}
        for linked_doc in package.linked_document_list:
            category = linked_doc.space
            group = linked_doc.groupedBy
            if category in classes.keys():
                classes[category] = classes[category] + 1
            else:
                classes[category] = 0

            if group in groupby_count.keys():
                groupby_count[group] = groupby_count[group] + 1
            else:
                groupby_count[group] = 0

        package.log_stage(
                "\nCategory Analysis" + "\nCategory map " + self.class_log(
                    classes) + "\n Groupby map" + "\n\n" + self.groupby_log(groupby_count))
        package.any_inputs_dict["Sample_By_Space"] = classes
        package.any_inputs_dict["Sample_By_Group"] = groupby_count
        return package

    def class_log(self, classes):
        class_string = "\n"
        for key, count in classes.items():
            class_string = class_string + str(key) + "\t" + str(count) + "\n"
        return class_string

    def groupby_log(self, groupby_count):

        groupby_string = "\n"
        for group, cnt in groupby_count.items():
            groupby_string = groupby_string + str(group) + "\t" + str(cnt) + "\n"
        return groupby_string

class EvenBySpace:
    def __init__(self):
        pass

    def perform(self, package: merm_model.PipelinePackage):
        categories_dict = package.any_inputs_dict["Sample_By_Space"]
        category_count_list = list(categories_dict.values())
        current_category_count = {}


        average = stats.mean(category_count_list)


        min_threshold = average * 0.3
        max_threshold = average * 1.1
        shuffle(package.linked_document_list)

        new_doc_list = []
        for linked_doc in package.linked_document_list:

            if categories_dict[linked_doc.space] < min_threshold:
                #package.linked_document_list.remove(linked_doc)
                continue
            elif categories_dict[linked_doc.space] > max_threshold:
                if linked_doc.space in current_category_count.keys():

                    if current_category_count[linked_doc.space] > max_threshold:
                        continue
                    else:
                        new_doc_list.append(linked_doc)
                        current_category_count[linked_doc.space] = current_category_count[linked_doc.space] + 1
                else:
                    current_category_count[linked_doc.space]  = 0
                    new_doc_list.append(linked_doc)
            else:
                new_doc_list.append(linked_doc)
        package.linked_document_list = new_doc_list
        package.log_stage("Removed categories with fewer than " + str(min_threshold) + " rows in category.\nReduced rows for categories with more than " + str(max_threshold) + " rows in category.")
        return package

class EvenByGroup:
    def __init__(self):
        pass

    def perform(self, package: merm_model.PipelinePackage):
        group_dict = package.any_inputs_dict["Sample_By_Group"]
        group_count_list = list(group_dict.values())
        current_category_count = {}


        median = stats.median(group_count_list)


        min_threshold = median * 0.5
        max_threshold = median * 0.9
        shuffle(package.linked_document_list)

        new_doc_list = []
        for linked_doc in package.linked_document_list:

            if group_dict[linked_doc.groupedBy] < min_threshold:
                #package.linked_document_list.remove(linked_doc)
                continue
            elif group_dict[linked_doc.groupedBy] > max_threshold:
                if linked_doc.groupedBy in current_category_count.keys():

                    if current_category_count[linked_doc.groupedBy] > max_threshold:
                        continue
                    else:
                        new_doc_list.append(linked_doc)
                        current_category_count[linked_doc.groupedBy] = current_category_count[linked_doc.groupedBy] + 1
                else:
                    current_category_count[linked_doc.groupedBy]  = 0
                    new_doc_list.append(linked_doc)
            else:
                new_doc_list.append(linked_doc)
        package.log_stage("Removed categories with fewer than " + str(
            min_threshold) + " rows in group.\nReduced rows for categories with more than " + str(max_threshold) + " rows in group.")
        package.linked_document_list = new_doc_list
        return package




