import logging
import os


def mkdirInCwd(dirname):
    if not os.path.exists(os.getcwd() + "/" + dirname):
        os.makedirs(os.getcwd() + "/" + dirname)
    return os.getcwd() + "/" + dirname

def getLogger():
    l = logging.getLogger('merm_tools')
    if l.handlers == []:
        l.setLevel(logging.INFO)
        # create file handler which logs even INFO messages
        logfile = str(mkdirInCwd("logs")) + "/_logfile.txt"
        fh = logging.FileHandler(logfile, mode="w")
        fh.setLevel(logging.INFO)
        # create console handler with a higher log level
        ch = logging.StreamHandler()
        ch.setLevel(logging.INFO)
        # create formatter and add it to the handlers
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        fh.setFormatter(formatter)
        ch.setFormatter(formatter)
        # add the handlers to the logger
        l.addHandler(fh)
        l.addHandler(ch)

    return (l)

def getReportLogger():
    l = logging.getLogger('merm_tools_reporter')
    if l.handlers == []:
        l.setLevel(logging.INFO)
        # create file handler which logs even INFO messages
        logfile = str(mkdirInCwd("logs")) + "/report.txt"
        fh = logging.FileHandler(logfile, mode="w")
        fh.setLevel(logging.INFO)
        # create console handler with a higher log level
        ch = logging.StreamHandler()
        ch.setLevel(logging.INFO)
        # create formatter and add it to the handlers
        formatter = logging.Formatter('%(message)s')
        fh.setFormatter(formatter)
        ch.setFormatter(formatter)
        # add the handlers to the logger
        l.addHandler(fh)
        l.addHandler(ch)

    return (l)
