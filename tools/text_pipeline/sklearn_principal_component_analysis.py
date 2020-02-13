
import tools.utils.log as log
import tools.model.model_classes as merm_model
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report, confusion_matrix, accuracy_score
import pandas as pd
from sklearn.decomposition import PCA
from sklearn.discriminant_analysis import LinearDiscriminantAnalysis as LDA
from datetime import datetime

class ScikitPrincipalComponentAnalysis:

    def __init__(self):
        pass

    def _analysis_id(self, package: merm_model.PipelinePackage):
        dt = datetime.now()
        suffix = str(dt.microsecond)[-4:]
        if "pca_iteration_count" in package.any_inputs_dict.keys():
            rf_count = package.any_inputs_dict["pca_iteration_count"] + 1
            package.any_inputs_dict["pca_iteration_count"] = rf_count
        else:
            rf_count = 0
            package.any_inputs_dict["pca_iteration_count"] = rf_count

        categories = package.any_inputs_dict["SKcategories"]
        category_count = len(list(categories.keys()))
        id =  "pca" + str(rf_count) + "_" + str(category_count)  + "_" + str(len(package.any_inputs_dict["SKY"])) + "_" + suffix
        return id

    def perform(self, package: merm_model.PipelinePackage):
        test_proportion = package.dependencies_dict["env"].config.getfloat("ml_instructions", "rf_test_proportion")
        random_state = 0
        sk_id  = self._analysis_id(package)
        package.any_inputs_dict["sk_last_id"] = sk_id

        rf_categories = package.any_inputs_dict["SKcategories"]
        X = package.any_inputs_dict["SKX"]

        pca = PCA(n_components=100)
        principalComponents = pca.fit_transform(X.toarray())

        print('Original number of features:', X.shape[1])
        print('Reduced number of features:', principalComponents.shape[1])
        explained_variance_ratio =pd.DataFrame(pca.explained_variance_ratio_).values.tolist()
        explained_variance = pd.DataFrame(pca.explained_variance_).values.tolist()
        components = pd.DataFrame(pca.components_.T).values.tolist()
        for component in components:
            for idx, contribution in enumerate(component):
                word = package.any_inputs_dict["SKdict"][idx]


        package.any_analysis_dict[sk_id + "_Ycategories"] = rf_categories
        package.any_analysis_dict[sk_id + "_explained_variance"] = explained_variance
        package.any_analysis_dict[sk_id + "_explained_variance_ratio"] = explained_variance_ratio
        package.any_analysis_dict[sk_id + "_components"] = components
        package.any_analysis_dict[sk_id +  "_vocab"] = package.any_inputs_dict["SKdict"]

        print(explained_variance)
        package.log_stage("Principal Component Analysis\nComponents: " + str(len(rf_categories) -1 )+  \
                           "\nOriginal number of features: " + str(X.shape[1]) +  \
                            "\n\nvariance  " + str(explained_variance))
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
