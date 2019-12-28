import tools.model.model_classes as merm_model
import pandas as pd
import gensim
import statistics
import tools.utils.log as log
class TopicLoader:

    def __init__(self):
        pass

    def perform(self, package:merm_model.PipelinePackage):

        lda_topics_by_subset_raw = self.load_topics_by_subset(package, "dict")
        lda_topics_toplevel_raw = self.load_top_level_topics(package, "dict")
        word_to_id = self.build_dict(lda_topics_by_subset_raw,lda_topics_toplevel_raw)

        lda_topics_by_subset_raw_byrow = self.load_topics_by_subset(package, "records")
        lda_topics_toplevel_raw_byrow = self.load_top_level_topics(package, "records")

        lda_topics_by_subset_raw_byrow_coded = self.code_terms(lda_topics_by_subset_raw_byrow, word_to_id)
        lda_topics_toplevel_raw_byrow_coded = self.code_terms(lda_topics_toplevel_raw_byrow, word_to_id)

        lda_topics_by_subset_formatted = self.reformat_data(lda_topics_by_subset_raw_byrow_coded)
        lda_topics_toplevel_formatted = self.reformat_data(lda_topics_toplevel_raw_byrow_coded)

        package.any_analysis_dict["lda_topics_by_subset_formatted"] = lda_topics_by_subset_formatted
        package.any_analysis_dict["lda_topics_toplevel_formatted"] = lda_topics_toplevel_formatted

        return merm_model.PipelinePackage(package.model,package.corpus,word_to_id,package.linked_document_list,
                                          package.any_analysis_dict, package.any_inputs_dict,
                                          package.dependencies_dict)

    def code_terms(self, lda_topics_byrow, word_to_id):
        for row in lda_topics_byrow:
            row["termidx"] = word_to_id[row["term"]]
        return lda_topics_byrow

    def reformat_data(self, lda_topics_byrow):
        new_format = {}
        for row in lda_topics_byrow:
            new_key = row["source"] + "_" + str(row["topic"])
            if new_key in new_format:
                topic_dict = new_format[new_key]
                topic_dict["term_indices"].append(row["termidx"])
                topic_dict["terms"].append(row["term"])
                topic_dict["weights"].append(row["weight"])
            else:
                topic_dict = {}
                topic_dict["term_indices"] = [row["termidx"]]
                topic_dict["terms"] = [row["term"]]
                topic_dict["weights"] = [row["weight"]]
                new_format[new_key] = topic_dict
        return new_format




    def build_dict(self, lda_topics_by_subset_raw, lda_topics_toplevel_raw):

        word_to_id = {}
        toplevel_terms = lda_topics_toplevel_raw["term"]
        subset_terms = lda_topics_by_subset_raw["term"]


        count = 0
        for idx, term in toplevel_terms.items():
            if not term in word_to_id:
                word_to_id[term] = count
                count = count + 1
        for idx, term1 in subset_terms.items():
            if not term1 in word_to_id:
                word_to_id[term1] = count
                count = count + 1
        return word_to_id

    def load_top_level_topics(self, package:merm_model.PipelinePackage, orientation):
        csv = package.dependencies_dict["env"].config["local_data"]["lda_topics_toplevel_raw"]
        df = pd.read_csv(csv)
        df.dropna(inplace=True)
        lda_topics_toplevel_raw = df.to_dict(orient = orientation)
        return lda_topics_toplevel_raw

    def load_topics_by_subset(self, package:merm_model.PipelinePackage, orientation):
        csv = package.dependencies_dict["env"].config["local_data"]["lda_topics_by_subset_raw"]
        df = pd.read_csv(csv)
        df.dropna(inplace=True)
        lda_topics_by_subset_raw = df.to_dict(orient = orientation)
        return lda_topics_by_subset_raw




class TopicComparator:

    def __init__(self):
        pass

    def perform(self, package:merm_model.PipelinePackage):
        lda_topics_by_subset_formatted = package.any_analysis_dict["lda_topics_by_subset_formatted"]
        lda_topics_toplevel_formatted = package.any_analysis_dict["lda_topics_toplevel_formatted"]
        similarity_dict = {}
        for source, topic_dict in lda_topics_toplevel_formatted.items():
            termidx_list = topic_dict["term_indices"]
            weight_list = topic_dict["weights"]
            tuples_list = list(zip(termidx_list, weight_list))
            result = self._similarity_score(lda_topics_by_subset_formatted, tuples_list)
            term_list  = topic_dict["terms"]
            result_dict = {}
            result_dict["terms"] = term_list
            result_dict["spaces"] = result
            similarity_dict[source] = result_dict

        package.any_analysis_dict["similarity_dict"] = similarity_dict
        return merm_model.PipelinePackage(package.model,package.corpus,package.dict,package.linked_document_list,
                                          package.any_analysis_dict,package.any_inputs_dict,
                                          package.dependencies_dict)




    def _similarity_score(self, lda_topics_by_subset_formatted, tuples_list):

        similarity_list = []
        for source, topic_dict in lda_topics_by_subset_formatted.items():
            termidx_list = topic_dict["term_indices"]
            weight_list = topic_dict["weights"]
            tuples_list_sub = list(zip(termidx_list, weight_list))
            sim = gensim.matutils.cossim(tuples_list, tuples_list_sub)
            #log.getLogger().debug(str(sim))
            similarity_list.append((sim, source))

        unzipped_similarity = list(zip(*similarity_list))
        length = len(unzipped_similarity[0])
        thesum = sum(unzipped_similarity[0])
        avg = thesum / length
        stdev = statistics.stdev(unzipped_similarity[0])
        threshold = avg + stdev


        msg = "\n\nAverage: " + str(avg) + "\n"
        msg1 = "StDev: " + str(stdev) + "\n"
        msg2 = "Threshold: " + str(avg + stdev) + "\n"

        log.getLogger().info(msg + msg1 + msg2)
        return self.related_topics(similarity_list,threshold)

    def related_topics(self, similarity_list, threshold):
        similarity_list_filtered = []
        for tuple in similarity_list:
            if tuple[0] > threshold:
                similarity_list_filtered.append(tuple)
        similarity_list_filtered.sort(reverse = True)

        return similarity_list_filtered


