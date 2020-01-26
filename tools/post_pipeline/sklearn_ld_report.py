import csv
import tools.model.model_classes as merm_model
import tools.utils.log as log
import tools.utils.envutils as env
from datetime import datetime
import collections



def run_post_process(package: merm_model.PipelinePackage):
    log.getLogger().info("save dictionaries to file")
    path = env.config["job_instructions"]["output_folder"]
    dt = datetime.now()
    suffix = str(dt.microsecond)[-4:]

    for key in package.any_analysis_dict:
        if "coefficient" in key:
            top_coefficients_dict = _coeffIcient_report_generator(package, key)
            _print_report(top_coefficients_dict,key,package.dependencies_dict["env"])

def _print_report(top_coefficients_dict, key, env):
    final_report = []
    final_report.append(["Component", "term" , "coefficient"])
    for component, tuple_list in top_coefficients_dict.items():
        for mytuple in tuple_list:
            final_report.append([component, mytuple[0], mytuple[1]])
    path = env.config["job_instructions"]["output_folder"]
    file_name = path + "/" + key + "ld_report.csv"
    env.list_of_lists_to_csv(file_name,final_report)

def _coeffIcient_report_generator(package: merm_model.PipelinePackage, key):
    key_components = key.split("_")
    if len(key_components) > 5:
        key_prefix = ""
        count = 0
        while count < 5:
          key_prefix = key_prefix+ key_components[count] + "_"
          count = count + 1

        analysis = package.any_analysis_dict[key]
        vocab_dict = package.any_analysis_dict[key_prefix + "vocab"]
        return _top_coefficients(analysis, vocab_dict)

def _top_coefficients(analysis, vocab_dict):
    results_by_component_dict = {}
    for component, coeff_list in enumerate(analysis):
        sorted_coeff_dict = _prep_coeff_dict(coeff_list)
        top_list = _one_component_top_words(sorted_coeff_dict, vocab_dict)
        results_by_component_dict[component] = top_list
    return results_by_component_dict

def _prep_coeff_dict(coeff_list):
    coeff_dict = {}
    for counter, coeff in enumerate(coeff_list):
        coeff_dict[abs(coeff)] = (counter, coeff)
    sorted_coeff_dict = collections.OrderedDict(sorted(coeff_dict.items(), reverse=True))
    return sorted_coeff_dict


def _one_component_top_words(sorted_coeff_dict, vocab_dict):
    count = 0
    top_list = []
    for key, tuple in sorted_coeff_dict.items():
        top_list.append((vocab_dict[tuple[0]], tuple[1]))
        count = count + 1
        if count == 50:
            break
    return top_list