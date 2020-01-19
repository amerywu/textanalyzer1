from sklearn.model_selection import GridSearchCV
from sklearn.ensemble import RandomForestClassifier
import tools.utils.log as log
import tools.model.model_classes as merm_model
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report, confusion_matrix, accuracy_score
import pandas as pd
from datetime import datetime

class ScikitRF:

    def __init__(self):
        pass

    def _analysis_id(self, package: merm_model.PipelinePackage):
        dt = datetime.now()
        suffix = str(dt.microsecond)[-4:]
        if "rf_iteration_count" in package.any_inputs_dict.keys():
            rf_count = package.any_inputs_dict["rf_iteration_count"] + 1
            package.any_inputs_dict["rf_iteration_count"] = rf_count
        else:
            rf_count = 0
            package.any_inputs_dict["rf_iteration_count"] = rf_count

        categories = package.any_inputs_dict["RFcategories"]
        category_count = len(list(categories.keys()))
        id = str(rf_count) + "_" + str(category_count)  + "_" + str(len(package.any_inputs_dict["RFY"])) + "_" + suffix
        return id

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
        classifier = RandomForestClassifier(n_estimators=1000, random_state=0)
        test_proportion = package.dependencies_dict["env"].config.getfloat("ml_instructions","rf_test_proportion")
        random_state = 0
        rf_dict = package.any_inputs_dict["RFdict"]

        rf_categories = package.any_inputs_dict["RFcategories"]
        X = package.any_inputs_dict["RFX"]
        y = package.any_inputs_dict["RFY"]
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size = test_proportion, random_state = random_state)
        classifier.fit(X_train, y_train)
        y_pred = classifier.predict(X_test)

        report = pd.DataFrame(confusion_matrix(y_test, y_pred)).values.tolist()
        report_string = self._report_string(report)
        analysis_id = self._analysis_id(package)
        package.any_inputs_dict["rf_last_id"] = analysis_id

        package.any_analysis_dict[analysis_id + "_classifier"] = classifier
        package.any_analysis_dict[analysis_id + "_confusion_pretty"] = self._pretty_confusion(rf_categories, report)
        package.any_analysis_dict[analysis_id + "_confusion"] = report
        package.any_analysis_dict[analysis_id + "_ypred"] = y_pred
        package.any_analysis_dict[analysis_id + "_ytest"] = y_test
        package.any_analysis_dict[analysis_id + "_Xtest"] = X_test
        package.any_analysis_dict[analysis_id + "_RFcategories"] = rf_categories

        package.log_stage(
            "\nTraining doc count: "+str(X_train.shape[0])+"\nTraining feature count: "+str(X_train.shape[1])+"\nTestTrain split:"+ str(test_proportion) + "\nRF confusion matrix:\n" + report_string + "\nclassification_report:\n" + str(classification_report(y_test, y_pred)) + "Accuracy:\n" + str(accuracy_score(y_test, y_pred)))
        return package

    def _report_string(self, report):
        report_string = "      "
        head_count = 0
        while head_count < len(report[0]):
            report_string = self._concatenate_number(head_count, report_string)
            head_count = head_count + 1
        report_string = report_string + "\n"
        row_count = 0

        for row in report:
            report_string = self._concatenate_number(row_count, report_string)
            for cell in row:
                report_string = self._concatenate_number(cell, report_string)
            row_count = row_count + 1
            report_string = report_string + "\n"
        return  report_string

    def _concatenate_number(self, number, report_string):
        new_string = report_string
        if number < 10:
            new_string = new_string + str(number) + "     "
        elif number < 100:
            new_string = new_string + str(number) + "    "
        elif number < 1000:
            new_string = new_string + str(number) + "   "
        elif number < 10000:
            new_string = new_string + str(number) + "  "
        else:
            new_string = new_string + str(number) + " "
        return  new_string




class ScikitRFSentenceFinder:

    def __init__(self):
        pass

    def _analysis_id(self, package: merm_model.PipelinePackage):
        last_id = package.any_inputs_dict["rf_last_id"]

        id = last_id + "_Finder_Sentences"
        return id





    def perform(self, package: merm_model.PipelinePackage):


        last_id = package.any_inputs_dict["rf_last_id"]
        y_test = package.any_analysis_dict[last_id + "_ytest"]
        y_pred = package.any_analysis_dict[last_id + "_ypred"]
        X_test = package.any_analysis_dict[last_id + "_Xtest"]



        rf_dict = package.any_inputs_dict["RFdict"]

        rf_categories = package.any_inputs_dict["RFcategories"]
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
            "\ndid some shit")
        return package

    def _add_to_sentence_list(self, major, pred_major, inv_rf_categories, near_missies_dict):

        if inv_rf_categories[major] in near_missies_dict.keys():
            if inv_rf_categories[pred_major] in near_missies_dict[inv_rf_categories[major]]:
                return True
        return False

class ScikitRFNearMisses:

    def __init__(self):
        pass

    def perform(self, package: merm_model.PipelinePackage):

        env = package.dependencies_dict["env"]
        near_miss_proportion = env.config.getfloat("ml_instructions","rf_near_miss_proportion")
        analysis_id = package.any_inputs_dict["rf_last_id"]
        confusion_key = analysis_id + "_confusion"
        category_key = analysis_id + "_RFcategories"
        rf_categories = package.any_analysis_dict[category_key]
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

        return package