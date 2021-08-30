from Controller import Controller

print("\nmonthly run FF")
category_name = "FF"
yaml_directory = "monthly_run.yaml"
monthly_run = Controller("monthly", yaml_directory, category_name,  'replace')
monthly_run.run_controller()

print("\nmonthly run DF")
category_name = "DF"
monthly_run = Controller("monthly", yaml_directory, category_name,  'append')
monthly_run.run_controller()

print("\nmonthly run BC")
category_name = "BC"
monthly_run = Controller("monthly", yaml_directory, category_name,  'append')
monthly_run.run_controller()

print("\nmonthly run HC")
category_name = "HC"
monthly_run = Controller("monthly", yaml_directory, category_name,  'append')
monthly_run.run_controller()