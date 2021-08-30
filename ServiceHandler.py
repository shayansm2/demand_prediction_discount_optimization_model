#%%
import services_config as conf
import pandas as pd
import datetime
import jdatetime
import yaml
#%%
class ServiceHandler (object):


    @classmethod
    def set_category(cls, category, append_or_replace) : 
        cls.category = category
        cls.append_or_replace = append_or_replace


    def __init__(self,  
                service_name, 
                output_directory = None, 
                input_directories = None):

        self.service_name = service_name
        self.output_directory = output_directory
        self.input_directories = input_directories

        self.input_dfs = None
        self.output_df = None
        self.prepare_input_data()
        self.execute_service()
        if (self.service_name != "insert_to_db") and (self.service_name != "insert_to_db_bi"):
            self.save_output_data()


    def prepare_input_data(self):

        if self.input_directories is not None:
            self.input_dfs = dict()

            for df_name in self.input_directories.keys():
                self.input_dfs[df_name] = pd.read_csv(self.input_directories[df_name])


    def execute_service(self):

        method_name = 'run_' + self.service_name
        class_method = getattr(self, method_name)
        class_method()


    def save_output_data(self):
        self.output_df.to_csv(self.output_directory, index = False)


    def run_get_sales_data(self):
        current_month = jdatetime.date.today().month
        months = (current_month - 7, current_month - 1)
        execute_mdx = True

        self.output_df = conf.get_monthly_sales_data(self.category, execute_mdx, months)
    

    def run_get_monthly_sales_data(self):
        current_month = jdatetime.date.today().month
        self.output_df = conf.get_monthly_sales_data(self.category, execute_mdx = True, months = (current_month - 1, current_month)) 


    def run_get_weekly_sales_data(self):
        self.output_df = conf.get_weekly_sales_data(self.category, execute_mdx = True) 
    

    def run_get_hourly_sales_weights(self):
        self.output_df = conf.get_hourly_sales_weights(self.category,execute_mdx = True) 


    def run_get_last_30_days_margin(self):
        self.output_df = conf.get_last_30_days_margin(self.category) 


    def run_get_last_30_days_pc1PerUnit_per_leafcat(self):
        self.output_df = conf.get_last_30_days_pc1PerUnit_per_leafcat(self.category) 


    #inputs should be sales_data : sales_directory
    def run_discount_bucket(self):

        sales_data = self.input_dfs["sales_data"]
        num_of_discount_clusters = 5
        current_year = jdatetime.date.today().year
        current_month = jdatetime.date.today().month
        # from_date = int(str(current_year) + str(current_month-3) + "01")
        # to_date = int(str(current_year) + str(current_month-1) + "30")
        from_date = 13991001
        to_date = 13991230
        start_end_data_discount_bucket = (from_date,to_date)
        # leaf_weights = pd.read_excel(r"data\Leafcat_weights.xlsx", sheet_name = "Sheet1")

        self.output_df = conf.discount_bucket(num_of_discount_clusters, 
                                                        sales_data, 
                                                        #leaf_weights, 
                                                        start_end_data_discount_bucket)


    # def run_get_hourly_stock_data(self):
    #     self.output_df = conf.get_hourly_stock_data(self.category,execute_sql = True)


    #inputs should be weights : hourly_sales_weights_directory
    def run_get_hourly_stock_data_weekly(self):
        weights = self.input_dfs["weights"]
        self.output_df = conf.get_hourly_stock_data(self.category, weights, prev_days = 7)


    #inputs should be weights : hourly_sales_weights_directory
    def run_get_hourly_stock_data_initial(self):
        weights = self.input_dfs["weights"]
        self.output_df = conf.get_hourly_stock_data(self.category, weights, prev_days = 7*30)


    #inputs should be weights : hourly_sales_weights_directory
    def run_get_hourly_stock_data_monthly(self):
        weights = self.input_dfs["weights"]
        self.output_df = conf.get_hourly_stock_data(self.category, weights, prev_days = 30)



    #inputs should be   sales_data : sales_directory
    #                   hourly_stock_data : hourly_stock_directory
    def run_add_hourly_stock(self):

        sales_data = self.input_dfs["sales_data"]
        hourly_stock_data = self.input_dfs["hourly_stock_data"]

        self.output_df = conf.add_hourly_stock(sales_data, hourly_stock_data)


    #inputs should be   sales_and_hourly_stock_data : directory
    #                   discount_bucket : directory
    def run_integrate_data(self):

        marketing_calendar = pd.read_excel(r"data\Calendar.xlsx", sheet_name = "DK")
        time_period_flag = {'1399-6':1,'1399-7':2,'1399-8':2,
                            '1399-9':2,'1399-10':3,'1399-11':3,
                            '1399-12':3, '1400-1':3}
        sales_and_hourly_stock_data = self.input_dfs["sales_and_hourly_stock_data"]
        discount_bucket_result = self.input_dfs["discount_bucket"]

        self.output_df = conf.integrate_data(sales_and_hourly_stock_data, 
                                                    discount_bucket_result, 
                                                    marketing_calendar, 
                                                    time_period_flag)


    # inputs should be integrate_data : directory   
    def run_initial_point_indicator(self):

        integrate_data_result = self.input_dfs["integrate_data"]
        self.output_df = conf.initial_point_indicator(integrate_data_result)
    

    # inputs should be integrate_data : directory
    def run_sales_aggregation(self):

        aggregation_level = ["Leaf_category","Brand"]
        integrate_data_result = self.input_dfs["integrate_data"]
        self.output_df = conf.sales_aggregation(integrate_data_result, aggregation_level)

    
    # inputs should be sales_affregation : directory
    def run_f_factor_calculation(self):

        sales_aggregation_result = self.input_dfs["sales_affregation"]
        self.output_df = conf.f_factor_calculation(sales_aggregation_result)


    def run_get_asrt_data(self):
        self.output_df = conf.get_asrt_data(self.category,execute_sql = True)
    

    #inputs should be   f_factor : directory
    #                   asrt_data : directory
    #                   initial_point : directory
    def run_optimum_discount_suggestion(self):

        asrt_data = self.input_dfs["asrt_data"]
        f_factor_result = self.input_dfs["f_factor"]
        initial_point_result = self.input_dfs["initial_point"]
        self.output_df = conf.optimum_discount_suggestion(asrt_data, f_factor_result, initial_point_result)
    

    # inputs should be optimum_discount_suggestion : directory
    def run_insert_to_db(self):

        optimum_discount_suggestion_results = self.input_dfs["optimum_discount_suggestion"]
        conf.insert_to_db(optimum_discount_suggestion_results)

    
    #inputs should be   initial_point_monthly : directory
    #                   initial_point_weekly : directory
    def run_initial_point_correction(self):

        initial_point_monthly = self.input_dfs["initial_point_monthly"]
        initial_point_weekly = self.input_dfs["initial_point_weekly"]
        self.output_df = conf.initial_point_correction(initial_point_monthly, initial_point_weekly)


    #inputs should be   f_factor : directory
    #                   f_factor_monthly : directory
    def run_f_factor_correction(self):

        f_factor_monthly = self.input_dfs["f_factor"]
        f_factor_weekly = self.input_dfs["f_factor_monthly"]
        self.output_df = conf.f_factor_correction(f_factor_monthly, f_factor_weekly)
    

    # inputs should be optimum_discount_suggestion : directory
    def run_insert_to_db_bi(self):

        optimum_discount_suggestion_results = self.input_dfs["optimum_discount_suggestion"]
        conf.insert_to_db_bi(optimum_discount_suggestion_results, self.append_or_replace)


    #inputs should be   optimum_discount_suggestion : directory
    #                   gross_margins : directory
    def run_add_abc_analysis_flag(self):

        optimum_discount_suggestion = self.input_dfs["optimum_discount_suggestion"]
        gross_margins = self.input_dfs["gross_margins"]
        self.output_df = conf.add_abc_analysis_flag(optimum_discount_suggestion, gross_margins)


    #inputs should be   optimum_discount_suggestion : directory
    #                   leafcats_gross_margins : directory
    def run_modify_discounts_by_pc1(self):
        optimum_discount_suggestion = self.input_dfs["optimum_discount_suggestion"]
        leafcats_gross_margins = self.input_dfs["leafcats_gross_margins"]
        self.output_df = conf.modify_discounts_by_pc1(optimum_discount_suggestion, leafcats_gross_margins)

