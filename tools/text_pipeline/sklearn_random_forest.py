from sklearn.model_selection import GridSearchCV
from sklearn.ensemble import RandomForestClassifier
import tools.utils.log as log
import tools.model.model_classes as merm_model
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report, confusion_matrix, accuracy_score


class ScikitRF:

    def __init__(self):
        pass

    def perform(self, package: merm_model.PipelinePackage):
        classifier = RandomForestClassifier(n_estimators=1000, random_state=0)
        test_proportion = package.dependencies_dict["env"].config.getfloat("ml_instructions","rf_test_proportion")
        random_state = 0
        X = package.any_inputs_dict["RFX"]
        y = package.any_inputs_dict["RFY"]
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size = test_proportion, random_state = random_state)
        classifier.fit(X_train, y_train)
        y_pred = classifier.predict(X_test)
        print("\nconfusion matrix:\n" + str(confusion_matrix(y_test, y_pred)))
        print("\nclassification report:\n" + str(classification_report(y_test, y_pred)))
        print("\naccuracy score:\n" + str(accuracy_score(y_test, y_pred)))
        print(str(package.any_inputs_dict["RFcategories"]))
        package.log_stage(
            "\nTraining doc count: "+str(X_train.shape[0])+"\nTraining feature count: "+str(X_train.shape[1])+"\nTestTrain split:"+ str(test_proportion) + "\nRF confusion matrix:\n" + str(confusion_matrix(y_test, y_pred)) + "\nclassification_report:\n" + str(classification_report(y_test, y_pred)) + "Accuracy:\n" + str(accuracy_score(y_test, y_pred)))
        return package



