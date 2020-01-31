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

def run_post_process(package: merm_model.PipelinePackage):
    log.getLogger().info("save dictionaries to file")
    path = env.config["job_instructions"]["output_folder"]
    dt = datetime.now()
    suffix = str(dt.microsecond)[-4:]

    for key in package.any_analysis_dict:

        analysis = package.any_analysis_dict[key]
        if  type(analysis) is dict:
            file_name = path +"/"+key+"_"+suffix+".csv"
            with open(file_name, 'w') as f:
                for k in analysis.keys():
                    _process_line(f, k, analysis)

def _process_line(f, k, analysis):
    if type(analysis) is dict:
        if type(analysis[k]) is list:
            for alist in analysis[k]:
                if type(alist) is list:
                    for element in alist:
                        line = key_to_string(k) + "," + key_to_string(element) + "\n"
                        _write(f,line)
                else:
                    line = key_to_string(k) + "," + key_to_string(alist) + "\n"
                    _write(f,line)
        elif type(analysis[k]) is dict:
            for key, value in analysis[k].items():
                if type(value) is list:
                    for element in value:
                        line = key_to_string(k) + "," + key_to_string(key) + "," + key_to_string(element) + "\n"
                        _write(f, line)
                else:
                    line = key_to_string(k) + "," +key_to_string(key) + "," + key_to_string(value) + "\n"
                    _write(f,line)
        else:
            line = key_to_string(k) +","+ key_to_string(analysis[k]) + "\n"
            _write(f, line)
    else:
        line = key_to_string(k) +","+key_to_string(analysis)+"\n"
        _write(f, line)


