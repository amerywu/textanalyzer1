
import tools.utils.log as log
import tools.model.model_classes as merm_model
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report, confusion_matrix, accuracy_score
import pandas as pd
from sklearn.discriminant_analysis import LinearDiscriminantAnalysis as LDA
from datetime import datetime

class ScikitLinearDiscriminantAnalysis:

    def __init__(self):
        pass

    def _analysis_id(self, package: merm_model.PipelinePackage):
        dt = datetime.now()
        suffix = str(dt.microsecond)[-4:]
        if "ld_iteration_count" in package.any_inputs_dict.keys():
            rf_count = package.any_inputs_dict["ld_iteration_count"] + 1
            package.any_inputs_dict["ld_iteration_count"] = rf_count
        else:
            rf_count = 0
            package.any_inputs_dict["ld_iteration_count"] = rf_count

        categories = package.any_inputs_dict["SKcategories"]
        category_count = len(list(categories.keys()))
        id =  str(rf_count) + "_" + str(category_count)  + "_" + str(len(package.any_inputs_dict["SKY"])) + "_" + suffix
        return id




    def perform(self, package: merm_model.PipelinePackage):
        test_proportion = package.dependencies_dict["env"].config.getfloat("ml_instructions", "rf_test_proportion")
        random_state = 0
        sk_id  = self._analysis_id(package)
        package.any_inputs_dict["sk_last_id"] = sk_id

        rf_categories = package.any_inputs_dict["SKcategories"]
        X = package.any_inputs_dict["SKX"]
        y = package.any_inputs_dict["SKY"]
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=test_proportion, random_state=random_state)
        lda = LDA(n_components=len(rf_categories) -1 )
        X_lda_trained = lda.fit_transform(X_train.toarray(), y_train)
        X_lda_tested = lda.transform(X_test.toarray())
        lda.fit(X_train.toarray(), y_train)
        y_pred = lda.predict(X_test.toarray())


        report = pd.DataFrame(confusion_matrix(y_test, y_pred)).values.tolist()
        report_string = self._report_string(report)
        package.any_analysis_dict[sk_id + "_confusion"] = report
        package.any_analysis_dict[sk_id + "_ypred"] = y_pred
        package.any_analysis_dict[sk_id + "_ytest"] = y_test
        package.any_analysis_dict[sk_id + "_Xtest"] = X_test
        package.any_analysis_dict[sk_id + "_Ycategories"] = rf_categories

        print('Accuracy' + str(accuracy_score(y_test, y_pred)))

        print('Original number of features:', X.shape[1])
        print('Reduced number of features:', X_lda_trained.shape[1])
        explained_variance = pd.DataFrame(lda.explained_variance_ratio_).values.tolist()
        coeff = pd.DataFrame(lda.coef_).values.tolist()

        print(explained_variance)
        package.log_stage("Linear Discriminat Analysis\nComponents: " + str(len(rf_categories) -1 )+ "\nAccuracy" + str(accuracy_score(y_test, y_pred)) + \
                           "\nOriginal number of features: " + str(X.shape[1]) + "\nReduced number of features:" +  str(X_lda_trained.shape[1]) + \
                            "\nConfusion matrix\n" + report_string)
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
