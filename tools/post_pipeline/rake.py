import csv
import tools.model.model_classes as merm_model
import tools.utils.log as log
import tools.utils.envutils as env


def run_post_process(package: merm_model.PipelinePackage):
    log.getLogger().info("rake post process")

    keywords_dict = package.any_analysis_dict["rake"]

    sorted_keywords_dict = _sortKeywords(keywords_dict)
    _saveToFile(sorted_keywords_dict)

def _saveToFile(sorted_keywords_dict):
    path = env.config["job_instructions"]["output_folder"]
    for key in sorted_keywords_dict.keys():

        dict_to_save = sorted_keywords_dict[key]
        filepath = str(path) + "/" + str(key) + "_rake.csv"
        csv_columns =["score","terms"]
        try:
            with open(filepath, 'w', newline='') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=csv_columns)
                writer.writeheader()
                for data in dict_to_save:
                    writer.writerow(data)
        except IOError:
            print("I/O error" + str(IOError))


def _sortKeywords(keywords_dict):
    output_dict = {}



    for key in keywords_dict.keys():

        keywords = keywords_dict[key][0]
        count = 1
        bymajor_list = []
        if keywords_dict[key][1] in output_dict.keys():
            bymajor_list = output_dict[keywords_dict[key][1]]
        else:
            output_dict[keywords_dict[key][1]] = bymajor_list



        for keyword_tuple in keywords:
            row_dict = {}
            row_dict["terms"] = keyword_tuple[0]
            row_dict["score"] = (keyword_tuple[1])

            if keyword_tuple[1] >=4:
                bymajor_list.append(row_dict)
            count = count + 1
    return output_dict
