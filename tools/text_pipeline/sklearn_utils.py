import tools.model.model_classes as merm_model

class ScikitPrettyConfusion:

    def __init__(self):
        pass


    def _pretty_confusion(self, rf_categories, confusion):
        pretty_confusion = []
        inv_rf_categories = {v: k for k, v in rf_categories.items()}
        pretty_confusion.append(list([""] + list(inv_rf_categories.values())))

        for idx, major in enumerate(self.normalize_confusion(confusion)):
            major_new = major.copy()
            major_new.insert(0, inv_rf_categories[idx])
            pretty_confusion.append(major_new)
        return pretty_confusion

    def normalize_confusion(self, confusion):
        normalized = []
        for idx, row in enumerate(confusion):
            row_sum = sum(row)
            normalized.append( list(map(lambda x: x / row_sum, row)))
        return normalized

    def perform(self, package: merm_model.PipelinePackage):
        last_id = package.any_inputs_dict["sk_last_id"]
        categories = package.any_analysis_dict[last_id + "_Ycategories"]
        confusion = package.any_analysis_dict[last_id + "_confusion"]
        pretty_confusion = self._pretty_confusion(categories,confusion)
        package.any_analysis_dict[last_id + "_pretty_confusion"] = pretty_confusion
        return package



class ScikitNearMisses:

    def __init__(self):
        pass

    def perform(self, package: merm_model.PipelinePackage):

        env = package.dependencies_dict["env"]
        near_miss_proportion = env.config.getfloat("ml_instructions","rf_near_miss_proportion")
        analysis_id = package.any_inputs_dict["sk_last_id"]
        confusion_key = analysis_id + "_confusion"
        rf_categories = package.any_inputs_dict["SKcategories"]
        inv_rf_categories = {v: k for k, v in rf_categories.items()}

        confusion = package.any_analysis_dict[confusion_key]

        near_miss_list = []
        near_miss_dict = {}
        for idx, row in enumerate(confusion):
            row_list =[]
            row_sum = sum(row)
            normalized = list(map(lambda x: x / row_sum, row))
            row_list.append(inv_rf_categories[idx])
            near_miss_dict[inv_rf_categories[idx]] = []
            for colidx, value in enumerate(normalized):
                if colidx != idx:
                    if value >= near_miss_proportion:
                        row_list.append(inv_rf_categories[colidx])
                        near_miss_dict[inv_rf_categories[idx]].append(inv_rf_categories[colidx])
            near_miss_list.append(row_list)

        package.any_analysis_dict[analysis_id + "_near_misses"] = near_miss_list
        package.any_analysis_dict[analysis_id + "_near_misses_dict"] = near_miss_dict
        package.log_stage("Identified 'near misses' i.e, high proportion of mis-categorized on one major. near miss including if proportion greater than " + str(near_miss_proportion))
        return package



class ScikitSentenceFinder:

    def __init__(self):
        pass

    def _analysis_id(self, package: merm_model.PipelinePackage):
        last_id = package.any_inputs_dict["sk_last_id"]

        id = last_id + "_Finder_Sentences"
        return id

    def perform(self, package: merm_model.PipelinePackage):
        last_id = package.any_inputs_dict["sk_last_id"]
        y_test = package.any_analysis_dict[last_id + "_ytest"]
        y_pred = package.any_analysis_dict[last_id + "_ypred"]
        X_test = package.any_analysis_dict[last_id + "_Xtest"]

        rf_dict = package.any_inputs_dict["SKdict"]

        rf_categories = package.any_inputs_dict["SKcategories"]
        inv_rf_categories = {v: k for k, v in rf_categories.items()}
        sentence_match_list = []
        sentence_match_list.append(["Actual", "Predicted", "Sentence", "Correct"])
        near_missies_dict = package.any_analysis_dict[last_id + "_near_misses_dict"]

        for idx, major in enumerate(y_test):

            pred_major = y_pred[idx]

            if pred_major == major:
                match = True
            else:
                match = False

            if match or self._add_to_sentence_list(major, pred_major, inv_rf_categories, near_missies_dict):
                sentence = X_test[[idx], :]
                #print(X_test[[idx], :])
                sentence_string = ""

                for word_idx in sentence.indices:
                    sentence_string = sentence_string + rf_dict[word_idx] + " "
                alist = [inv_rf_categories[major],inv_rf_categories[pred_major], sentence_string, match]
                sentence_match_list.append(alist)

        analysis_id = self._analysis_id(package)

        package.any_analysis_dict[analysis_id + "_sentences"] = sentence_match_list


        package.log_stage(
            "Found sentences that accurately predict each major or were near misses")
        return package

    def _add_to_sentence_list(self, major, pred_major, inv_rf_categories, near_missies_dict):

        if inv_rf_categories[major] in near_missies_dict.keys():
            if inv_rf_categories[pred_major] in near_missies_dict[inv_rf_categories[major]]:
                return True
        return False
