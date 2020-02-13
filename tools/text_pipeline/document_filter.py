import tools.model.model_classes as merm_model
import tools.utils.log as log
import statistics as stats
from random import shuffle

class SubsetData:

    def __init__(self):
        pass

    def _by_group(self, package):
        group_testenv = package.dependencies_dict["env"].config.getboolean("pipeline_instructions", "group_testenv")
        if True == group_testenv:
            relevant_groups_string = package.dependencies_dict["env"].config["pipeline_instructions"][
                "group_testenv_list"]
            relevant_groups_list = relevant_groups_string.split(",")
            return self._group_test_groups(package, relevant_groups_list)
        else:
            return self._group_all_groups(package)

    def _by_space(self, package):
        group_testenv = package.dependencies_dict["env"].config.getboolean("pipeline_instructions", "group_testenv")
        if True == group_testenv:
            relevant_groups_string = package.dependencies_dict["env"].config["pipeline_instructions"]["group_testenv_list"]
            relevant_groups_list = relevant_groups_string.split(",")
            return self._group_test_spaces(package, relevant_groups_list)
        else:
            return self._group_all_spaces(package)

    def perform(self, package: merm_model.PipelinePackage):
        thetype = type(package.linked_document_list)
        if thetype is dict:
            return package
        env = package.dependencies_dict["env"]
        by_space = env.config.getboolean("ml_instructions", "subset_by_space")

        if by_space == True:
            linked_doc_by_index = self._by_space(package)
        else:
            linked_doc_by_index = self._by_group(package)

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


    def _group_all_spaces(self, package):
        linked_doc_by_index = {}
        for linked_doc in package.linked_document_list:

            if linked_doc.space in linked_doc_by_index:
                linked_doc_by_index[linked_doc.space].append(linked_doc)
            else:
                space_list = []
                space_list.append(linked_doc)
                linked_doc_by_index[linked_doc.space] = space_list

        return linked_doc_by_index


    def _group_test_spaces(self, package, relevant_groups_list):
        linked_doc_by_index = {}
        for linked_doc in package.linked_document_list:

            if linked_doc.space in relevant_groups_list:

                if linked_doc.space in linked_doc_by_index:
                    linked_doc_by_index[linked_doc.space].append(linked_doc)
                else:
                    space_list = []
                    space_list.append(linked_doc)
                    linked_doc_by_index[linked_doc.space] = space_list

        return linked_doc_by_index


class StopWordRemoval:

    def __init__(self):
        pass

    def _stop_word_key(self, package):
        colutils = package.dependencies_dict["colutils"]
        key = colutils.get_top_incrementing_key("stop_words", package.any_analysis_dict)
        return key

    def perform(self, package: merm_model.PipelinePackage):


        log.getLogger().info("StopWordRemoval")
        stop_words = []
        load_source = ""
        if self._stop_word_key(package) in package.any_analysis_dict:
            log.getLogger().info("Stop words retrieved from package analyses.")
            log.getLogger().debug("It's a list")
            stop_words = package.any_analysis_dict[self._stop_word_key(package)]

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
            for word in linked_doc.tokens:
                if  word in stop_words:
                    # log.getLogger().debug("removing " + word)
                    linked_doc.tokens.remove(word)

        package.log_stage(
            "Removed all stop words from the corpus tokens (i.e., bag of words). The stop words were loaded " + load_source +"\nStop word count is " + str(len(stop_words)))
        return package


class ExcludeBySpace:

    def __init__(self):
        pass

    def perform(self, package: merm_model.PipelinePackage):
        thetype = type(package.linked_document_list)
        if thetype is dict:
            return package


        include_list = package.dependencies_dict["env"].config["ml_instructions"]["filter_space_include"].split(",")
        exclude_list = package.dependencies_dict["env"].config["ml_instructions"]["filter_space_exclude"].split(",")

        included = self.include_docs(include_list, package.linked_document_list)
        new_linked_doc_list = self.exclude_list(exclude_list,included)
        log_include_result = "\nAfter include filter, remaining docs: " + str(len(new_linked_doc_list)) +"\n"



        new_package = merm_model.PipelinePackage(package.model, package.corpus, package.dict, new_linked_doc_list,
                                                 package.any_analysis_dict, package.any_inputs_dict,
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

        include_list = package.dependencies_dict["env"].config["ml_instructions"]["filter_group_include"].split(",")
        exclude_list = package.dependencies_dict["env"].config["ml_instructions"]["filter_group_exclude"].split(",")
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
            "\nInclude filter was: " + str(include_list) + "\nExclude filter was:" + str(exclude_list) + "\nRemaining documents count: " + str(
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
        package.uncache_linked_docs()
        linked_docs = package.linked_document_list.copy()
        category_count_list = list(categories_dict.values())
        current_category_count = {}

        env = package.dependencies_dict["env"]
        dynamic_threshold = env.config.getboolean("ml_instructions", "filter_category_threshold_type_dynamic")
        median = stats.median(category_count_list)

        if dynamic_threshold == True:
            min = env.config.getfloat("ml_instructions","filter_category_min_threshold_dynamic")
            max = env.config.getfloat("ml_instructions", "filter_category_max_threshold_dynamic")
            min_threshold = median * min
            max_threshold = median * max
        else:
            min = env.config.getint("ml_instructions", "filter_category_min_threshold")
            max = env.config.getint("ml_instructions", "filter_category_max_threshold")
            min_threshold = min
            max_threshold = max
        shuffle(package.linked_document_list)

        new_doc_list = []
        for linked_doc in linked_docs:

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

        package.cache_linked_docs()
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
        env = package.dependencies_dict["env"]

        package.uncache_linked_docs()
        linked_docs = package.linked_document_list.copy()
        dynamic_threshold = env.config.getboolean("ml_instructions", "filter_groupby_threshold_type_dynamic")


        median = stats.median(group_count_list)


        if dynamic_threshold == True:
            min = env.config.getfloat("ml_instructions","filter_groupby_min_threshold_dynamic")
            max = env.config.getfloat("ml_instructions", "filter_groupby_max_threshold_dynamic")
            min_threshold = median * min
            max_threshold = median * max
        else:
            min = env.config.getint("ml_instructions", "filter_groupby_min_threshold")
            max = env.config.getint("ml_instructions", "filter_groupby_max_threshold")
            min_threshold = min
            max_threshold = max


        shuffle(linked_docs)

        new_doc_list = []
        for linked_doc in linked_docs:

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

        package.cache_linked_docs()
        package.linked_document_list = new_doc_list
        return package


class RemoveDuplicateDocs:
    def __init__(self):
        pass

    def perform(self, package: merm_model.PipelinePackage):

        original_count = len(package.linked_document_list)
        filter_dict = {}
        for linked_doc in package.linked_document_list:
            filter_dict[linked_doc.raw] = linked_doc

        doc_list = list(filter_dict.values())
        package.linked_document_list = doc_list

        package.log_stage("Before removing duplicates:  " + str(original_count) +"\nAfter removing duplicates: " + str(len(package.linked_document_list)))

        return package

class Reset:
    def __init__(self):
        pass

    def perform(self, package: merm_model.PipelinePackage):
        original_linked_doc_size = len(package.linked_document_list)
        package.uncache_linked_docs()

        package.log_stage("Original linked doc count: " + str(original_linked_doc_size) + "Current linked doc count: " + str(len(package.linked_document_list)))
        return package

class FilterTokensByCount:
    def __init__(self):
        pass

    def perform(self, package: merm_model.PipelinePackage):
        env = package.dependencies_dict["env"]
        original_count = len(package.linked_document_list)
        min_length = env.config.getint("ml_instructions", "min_sentence_length")
        sentence_list = []
        for doc in package.linked_document_list:
            if len(doc.tokens) >= min_length:
                sentence_list.append(doc)
        package.linked_document_list = sentence_list

        package.log_stage("Removed sentences with fewer than " + str(min_length) + " tokens. \n Original doc count: " + str(original_count) + "\nNew doc count: " + str(len(package.linked_document_list)) )
        return package

class CombineSentencesToDocs:
    def __init__(self):
        pass

    def perform(self, package: merm_model.PipelinePackage):
        env = package.dependencies_dict["env"]
        original_count = len(package.linked_document_list)
        merge_by = env.config["ml_instructions"]["merge_docs_field"]
        merged_docs_dict = {}


        for sub_doc in package.linked_document_list:
            if merge_by == "groupedBy":
                key = sub_doc.groupedBy
            else:
                key = sub_doc.space

            if key in merged_docs_dict.keys():
                merged_docs_dict[key]
                merged_docs_dict[key].raw = merged_docs_dict[key].raw + " " + sub_doc.raw
                merged_docs_dict[key].tokens = merged_docs_dict[key].tokens + sub_doc.tokens
            else:
                merged_docs_dict[key] = sub_doc

        new_linked_doc_list = list(merged_docs_dict.values())
        package.linked_document_list = new_linked_doc_list


        package.log_stage("Merged documents by " + str(merge_by) + " tokens. \n Original doc count: " + str(original_count) + "\nNew doc count: " + str(len(package.linked_document_list)) )
        return package
