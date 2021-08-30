# %%
import yaml
import datetime
from ServiceHandler import ServiceHandler


class Controller(object):

    def __init__(self, controller_name, yaml_file_name, category_name, append_or_replace):

        self.controller_name = controller_name
        stream = open(yaml_file_name, 'r')
        self.process = yaml.load(stream)
        self.category = category_name
        self.append_or_replace = append_or_replace

    def run_controller(self):

        ServiceHandler.set_category(self.category, self.append_or_replace)

        for service in self.process.keys():

            print(datetime.datetime.now(), "\t", service, "Started")
            output_directory = str(self.process[service]["directory"]).format(self.category)
            inputs = self.process[service]["inputs"]
            if inputs is not None:
                for input_name in inputs.keys():
                    inputs[input_name] = str(inputs[input_name]).format(self.category)
            ServiceHandler(service, output_directory, inputs)
            print(datetime.datetime.now(), "\t", service, "Ended")
