import tools.model.model_classes as merm_model
import tools.utils.log as log
import tools.utils.envutils as env

class SaveDfAsCsv:

    def __init__(self):
        pass

    def perform(self,package:merm_model.PipelinePackage):
        package.corpus.to_csv(env.config['job_instructions']['es_file_location'],index=False)
        log.getLogger().info("Saved ElasticSearch Data as CSV at: "+env.config['job_instructions']['es_file_location'])
        package.log_stage("Saved ElasticSearch Data as CSV at: "+env.config['job_instructions']['es_file_location'])
        return package





