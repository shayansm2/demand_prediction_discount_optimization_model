from Controller import Controller

print("initial run FF")
category_name = "HC"
yaml_directory = "initial_run.yaml"
initial_run = Controller("initial", yaml_directory, category_name, 'replace')
initial_run.run_controller()

print("\ninitial run DF")
category_name = "DF"
yaml_directory = "initial_run.yaml"
initial_run = Controller("initial", yaml_directory, category_name, 'append')
initial_run.run_controller()