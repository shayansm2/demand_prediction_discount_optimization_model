from Controller import Controller

print("daily run FF")
category_name = "FF"
yaml_directory = "daily_run.yaml"
daily_run = Controller("daily", yaml_directory, category_name, 'replace')
daily_run.run_controller()

print("\ndaily run DF")
category_name = "DF"
daily_run = Controller("daily", yaml_directory, category_name, 'append')
daily_run.run_controller()

print("\ndaily run BC")
category_name = "BC"
daily_run = Controller("daily", yaml_directory, category_name, 'append')
daily_run.run_controller()

print("\ndaily run HC")
category_name = "HC"
daily_run = Controller("daily", yaml_directory, category_name, 'append')
daily_run.run_controller()