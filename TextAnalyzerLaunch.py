
import tools.elasticsearch_management.es_extraction as es_extract
import tools.utils.envutils as env
import tools.utils.log as log
import tools.pipeline_framework.factory as factory
import tools.pipeline_framework.pipeline_process as pipe_process
import tools.pipeline_framework.pipeline as pipeline
import tools.utils.text_parsing_utils as utils
import tools.utils.dfutils as dfutils
import tools.utils.collectionutils as colutils

def initiate_run():
    try:
        log.getLogger().info(env.printEnvironment())
        env.init()
        log.getLogger().info(env.printConf())
        continue_run = True

        dependencies_dict = {}
        dependencies_dict["env"] = env
        dependencies_dict["factory"] = factory
        dependencies_dict["es_extract"] = es_extract
        dependencies_dict["pipe_process"] = pipe_process
        dependencies_dict["utils"] = utils
        dependencies_dict["dfutils"] = dfutils
        dependencies_dict["colutils"] = colutils

        while continue_run == True:
            es_extract.initiate_extraction(pipeline.run_pipeline, dependencies_dict)
            continue_run = env.continue_run()
            if(not env.run_forever()):
                break
        log.getLogger().info("#################### Run Completed :) #################### ")

    except Exception as e:
        msg = str(e)
        log.getLogger().error(env.print_traceback())
        log.getLogger().error(msg)

initiate_run()




