from Controller import Controller

category_name = "BC"
yaml_directory = "daily_run.yaml"
test = Controller("test", yaml_directory, category_name, append_or_replace = "append")
test.run_controller()