from Controller import Controller

print("\nweekly run FF")
category_name = "FF"
yaml_directory = "weekly_run.yaml"
weekly_run = Controller("weekly", yaml_directory, category_name,  'replace')
weekly_run.run_controller()

print("\nweekly run DF")
category_name = "DF"
weekly_run = Controller("weekly", yaml_directory, category_name,  'append')
weekly_run.run_controller()


print("\nweekly run BC")
category_name = "BC"
weekly_run = Controller("weekly", yaml_directory, category_name,  'append')
weekly_run.run_controller()


print("\nweekly run HC")
category_name = "HC"
weekly_run = Controller("weekly", yaml_directory, category_name,  'append')
weekly_run.run_controller()