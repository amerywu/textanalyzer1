import configparser
import os
import sys
import traceback
from os.path import expanduser


config = configparser.ConfigParser()
config_a = configparser.ConfigParser()


def init():
    print(str(workingDir()))
    path = str(workingDir()) + "/resources/tools.ini"
    print(path)
    path1 = str(workingDir()) + "/resources/tonglu.ini"
    print(path1)
    config.read(str(workingDir()) + "/resources/tools.ini")
    config_a.read(str(workingDir()) + "/resources/tonglu.ini")


def run_forever():
    if str(config["pipeline_instructions"]["run_forever"]).lower() == "yes":
        return True
    elif str(config["pipeline_instructions"]["run_forever"]).lower() == "true":
        return True
    else:
        return False

def continue_run():
    cnf =configparser.ConfigParser()

    path = str(workingDir()) + "/resources/tools.ini"
    cnf.read(str(path))
    cnf.update

    cr = cnf["pipeline_instructions"]["continue_run"]
    if( str(cr).lower() == "true"  ):

        return True
    else:

        return False



def printConf():
    confstring = "\n\n-- tools.ini --\n"
    for section in config.sections():
        confstring += "\n[" + section + "]\n"
        for key in config[section]:
            confstring += key + " = " + config[section][key] + "\n"
    return confstring

def workingDir():
    wd = os.getcwd()
    return os.getcwd()

def printEnvironment():
    currentdir = "Current Dir: " + str(os.getcwd()) + str(os.fspath)
    homedir = "Home Dir: " + str(expanduser("~"))
    interpreter = "Interpreter: " + str(sys.executable)
    version = "Version: Python " + str(sys.version_info)
    return "\n" + currentdir + "\n" + homedir + "\n" + interpreter + "\n" + version


def mkdirInCwd(dirname):
    if not os.path.exists(os.getcwd() + "/" + dirname):
        os.makedirs(os.getcwd() + "/" + dirname)
    return os.getcwd() + "/" + dirname


def mkdir(extantPath, newdir):
    if not os.path.exists(extantPath + "/" + newdir):
        os.makedirs(extantPath + "/" + newdir)
    return os.makedirs(extantPath + "/" + newdir)

def print_traceback(header ="\n---- EXCEPTION ----\n", footer = "\n------------\n", dev_only = False):
    if test_env() or not dev_only:
        msg = header + traceback.format_exc() + footer
        return msg

def test_env():
    if str(config["pipeline_instructions"]["testenv"]).lower() == "yes":
        return True
    elif str(config["pipeline_instructions"]["testenv"]).lower() == "true":
        return True
    else:
        return False

def test_env_doc_processing_count():
    str_count = config["pipeline_instructions"]["testenv_doc_process_count"]
    return int(str_count)

def read_file(path):
    print("Opening file: " + str(path))
    f = open(path, "r")
    text = f.read()
    return text

def overwrite_file(path, text):
    print("Writing file "+ str(path))
    f = open(path, "w")
    f.write(text)
    f.close()

