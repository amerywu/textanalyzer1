import tools.pipeline_framework.factory as factory
import tools.utils.log as log

class PipelineFactory:

    step_count = 0

    def next_step(self,task:str, input):

        self.step_count = self.step_count +1
        msg = "\n\nEntering " + task + " " + str(self.step_count) + "\n\n"
        log.getLogger().info(msg)
        manifest = factory.PipelineManifest
        new_task = manifest.manifest[task]
        result = new_task.perform(input)
        if("Package" in type(result).__name__):
            log.getLogger().warning("STRUCTURE after " + task + ": " + result.structure())
        else:
            log.getLogger().warning("The return type is not of type PipelinePackage. THIS IS BAD PRACTICE :(")
        return result
