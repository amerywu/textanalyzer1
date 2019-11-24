import csv
import tools.model.model_classes as merm_model
import tools.utils.log as log
import tools.utils.envutils as env
import tools.utils.es_connect as es_conn
import json


def run_post_process(package: merm_model.PipelinePackage):
    log.getLogger().info("save dictionaries to file")
    _process_major_final(package)
    _process_major_final_from_job_title(package)





def _process_major_final_from_job_title(package):
    job_to_major_path = env.config["local_data"]["job_to_majors_filepath"]
    json1_file = open(job_to_major_path)
    json1_str = json1_file.read()
    job_to_major_dict = json.loads(json1_str)


    for index, row in package.corpus.iterrows():
        majorFinal = row["majorFinal"]

        if majorFinal is None:
            job = row["jobFinal"]
            if job is not None:
                jobupper = job.upper()
                if jobupper in job_to_major_dict and majorFinal is None:
                    major_final_from_file = job_to_major_dict[jobupper]
                    package.corpus.loc[index, "majorFinal"] = major_final_from_file
                    log.getLogger().info(major_final_from_file)
                    log.getLogger().info("_process_major_final_from_job_title added to  df: " + str(package.corpus.loc[index, "majorFinal"]))
                    doc_id = row["id"]
                    #_generate_json_and_dispatch(doc_id,row["indexname"], major_final_from_file)



def _process_major_final(package):
    aggregated_majors_path = env.config["local_data"]["aggregated_majors_filepath"]
    json1_file = open(aggregated_majors_path)
    json1_str = json1_file.read()
    aggregated_majors_dict = json.loads(json1_str)

    for index, row in package.corpus.iterrows():
        majorFinal = row["majorFinal"]

        if majorFinal is None:
            areas_of_study = row["areasOfStudy"]
            if len(areas_of_study) > 0:
                areasOfStudyList = areas_of_study.split(",")
                for s in areasOfStudyList:
                    supper = s.upper()
                    if supper in aggregated_majors_dict and majorFinal is None:
                        major_final_from_file = aggregated_majors_dict[supper]
                        package.corpus.loc[index, "majorFinal"] = major_final_from_file
                        log.getLogger().info(major_final_from_file)
                        log.getLogger().info("added to  df: " + str(package.corpus.loc[index, "majorFinal"]))
                        majorFinal = major_final_from_file
                        doc_id = row["id"]
                        _generate_json_and_dispatch(doc_id, row["indexname"], major_final_from_file)


def _generate_json_and_dispatch(id, index_name, major, retry_count = 0):
    try:

        es = es_conn.connectToES()
        response = es.update(index=index_name,  id=id, body={"doc": {"majorFinal":major}})
        log.getLogger().info("Updating "+ id + " with " + major)
        print ('response:', response)


    except Exception as e:
        retry_count = retry_count + 1
        msg = "WARN: " +  str(e)

        log.getLogger().error(msg)
        if "time" in msg.lower() and retry_count < 10:
            _generate_json_and_dispatch(id,index_name,major,retry_count)
        else:
            pass
    #log.getLogger().info("Dispatched " + str(total_sentences))
