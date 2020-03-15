import csv
import tools.model.model_classes as merm_model
import tools.utils.log as log
import tools.utils.envutils as env
from datetime import datetime


def _write(f, line):
    f.write(line)

def key_to_string(k):
    if type(k) is list or type(k) is tuple:
        s = ""
        for e in k:
            s = s + " " + str(e)
        return s
    else:
        return str(k)

def list_to_csv(l):
    if type(l) is list or type(l) is tuple:
        s = ""
        for e in l:
            s = s + " " + str(e)
        return s
    else:
        return str(l)

def _file_names(analysis, path, key, suffix):
    dimensions = env.config.getint("ml_instructions", "glove_dimensions")
    files = {}
    for k in analysis.keys():
        files[k] = open(path +"/_"+ str(dimensions) + "d_" + key + "_" + k + "_" +suffix+".csv", 'w')
    return files



def run_post_process(package: merm_model.PipelinePackage):
    log.getLogger().info("save dictionaries to file")
    path = env.config["job_instructions"]["output_folder"]
    dt = datetime.now()
    suffix = str(dt.microsecond)[-4:]

    all_loadings = {}
    for key in package.any_analysis_dict:
        analysis = package.any_analysis_dict[key]
        if  type(analysis) is dict and len(analysis) > 0 and "glove" in key.lower():
            files = _file_names(analysis, path, key, suffix)
            for k in analysis.keys():
                _process_line(files[k], k, analysis)
        elif type(analysis) is list and "glove" in key.lower():
            _process_list(analysis, suffix, path, key)
            if "loadings" in key.lower():
                all_loadings[key] = analysis
    if len(all_loadings) > 0:
        formatted_loadings = _format_all_loadings(all_loadings)
        _process_list(formatted_loadings,suffix, path, "_loading_comparison")

def _format_all_loadings(all_loadings):
    uber_list = []
    for analysis_key, thelist in all_loadings.items():
        for didx, dimension in enumerate(thelist):
            if len(uber_list) <= didx:
                uber_list.append([])
            for idx, entry in enumerate(dimension):
                entry[0]= analysis_key + "," + entry[0]
                if len(uber_list[didx]) <= idx:
                    uber_list[didx].append([])
                uber_list[didx][idx] = uber_list[didx][idx] + entry + [","]
    return uber_list






def _process_dict(f, analysis):
    for k in analysis.keys():
        _process_line(f, k, analysis)

def _process_list(analysis, suffix, path, key):
    if type(analysis[0]) is list:
        separator = " "
        file_name = path + "/" + key + "_" + suffix + ".csv"
        if len(analysis[0]) > 0 and type(analysis[0][0]) is list:
            with open(file_name, "w", newline="") as f:
                for rows in analysis:
                    writer = csv.writer(f, delimiter=separator)
                    writer.writerows(rows)
        else:
            with open(file_name, "w", newline="") as f:
                writer = csv.writer(f, delimiter=separator)
                writer.writerows(analysis)


def _line_is_list(analysis, k, f):
    for alist in analysis[k]:
        if type(alist) is list:
            if type(alist[0]) is list:
                for sublist in alist:
                    line = ""
                    for entry in sublist:
                        line = line + str(entry) + ","
                    _write(f, line + "\n")
            else:
                line = list_to_csv(alist) + "\n"
                _write(f, line)
        else:
            line = key_to_string(k) + "," + key_to_string(alist) + "\n"
            _write(f, line)

def _line_is_dict(analysis, k, f):
    for key, value in analysis[k].items():
        if type(value) is list:
            for element in value:
                if type(element) is list:
                    row_string = k + "," + key_to_string(key) + "," + list_to_csv(element) + "\n"
                    _write(f, row_string)
                else:
                    line = key_to_string(k) + "," + key_to_string(key) + "," + key_to_string(element) + "\n"
                    _write(f, line)
        else:
            line = key_to_string(k) + "," + key_to_string(key) + "," + key_to_string(value) + "\n"
            _write(f, line)

def _process_line(f, k, analysis):
    if type(analysis) is dict:
        if type(analysis[k]) is list:
            _line_is_list(analysis, k, f)

        elif type(analysis[k]) is dict:
            _line_is_dict(analysis, k, f)
    else:
        line = key_to_string(k) + "," + key_to_string(analysis) + "\n"
        _write(f, line)


