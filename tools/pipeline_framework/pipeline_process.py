import tools.pipeline_framework.factory as factory
import tools.utils.log as log
import tools.model.model_classes as merm_model

class PipelineFactory:

    step_count = 0

    def next_step(self, task:str, package:merm_model.PipelinePackage):

        self.step_count = self.step_count + 1
        msg = "\n\nEntering " + task + " " + str(self.step_count) + "\n\n"
        log.getLogger().info(msg)
        manifest = factory.PipelineManifest
        new_task = manifest.manifest[task]
        result = new_task.perform(package)
        package.any_inputs_dict["previous_task"] = task
        package.any_inputs_dict["history"].append(task)
        if("Package" in type(result).__name__):
            log.getLogger().warning("STRUCTURE after " + task + ": " + result.structure())
        else:
            log.getLogger().warning("The return type is not of type PipelinePackage. THIS IS BAD PRACTICE :(")
        return result