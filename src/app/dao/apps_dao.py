from . import bq_client
from app.custom_logging.log_decorator import log_decorate_class


# Currently there are no dimensions in the app_browser_afk_flat table. Still top_n queries are written to handle group by dimensions, if required in the future.
class AppsDAO:
    def __init__(self):
        self.table_name = "app_browser_afk_flat"

    def query_apps_groupby(self, start_ts, end_ts, start_ts_lag, end_ts_lag, group_by_dim_str,
                            group_by_cols_tw_str, lag=1, filter_condition=''):
        return f"""
           SELECT {group_by_dim_str}, {group_by_cols_tw_str}, concat({group_by_cols_tw_str}) ts, COUNT(1) as data, 0 lag             
           from calcium-sunbeam-341614.rtp_solution.{self.table_name}  
           where end_timestamp <= "{end_ts}" and start_timestamp >= "{start_ts}" {filter_condition}      
           group by {group_by_dim_str}, {group_by_cols_tw_str}
           union all
           SELECT {group_by_dim_str}, {group_by_cols_tw_str}, concat({group_by_cols_tw_str}) ts, COUNT(1) as data, {lag} lag             
           from calcium-sunbeam-341614.rtp_solution.{self.table_name}  
           where end_timestamp <= "{end_ts_lag}" and start_timestamp >= "{start_ts_lag}" {filter_condition}    
           group by {group_by_dim_str}, {group_by_cols_tw_str}
           """

    def query_apps_duration_groupby(self, start_ts, end_ts, start_ts_lag, end_ts_lag, group_by_dim_str,
                            group_by_cols_tw_str, lag=1, filter_condition=''):
        return f"""
           SELECT {group_by_dim_str}, {group_by_cols_tw_str}, concat({group_by_cols_tw_str}) ts, sum(time_diff(duration,"00:00:00",SECOND)) as data, 0 lag             
           from calcium-sunbeam-341614.rtp_solution.{self.table_name}  
           where end_timestamp <= "{end_ts}" and start_timestamp >= "{start_ts}" {filter_condition}      
           group by {group_by_dim_str}, {group_by_cols_tw_str}
           union all
           SELECT {group_by_dim_str}, {group_by_cols_tw_str}, concat({group_by_cols_tw_str}) ts, sum(time_diff(duration,"00:00:00",SECOND)) as data, {lag} lag             
           from calcium-sunbeam-341614.rtp_solution.{self.table_name}  
           where end_timestamp <= "{end_ts_lag}" and start_timestamp >= "{start_ts_lag}" {filter_condition}    
           group by {group_by_dim_str}, {group_by_cols_tw_str}
           """

    def query_apps_total_active_duration(self, start_ts, end_ts, start_ts_lag, end_ts_lag, group_by_cols_tw_str, lag=1,
                         filter_condition=''):
        return f"""
        SELECT {group_by_cols_tw_str}, concat({group_by_cols_tw_str}) ts, sum(time_diff(duration,"00:00:00",SECOND)) as data, 0 lag
        from calcium-sunbeam-341614.rtp_solution.{self.table_name}  
        where end_timestamp <= "{end_ts}" and start_timestamp >= "{start_ts}" 
        and source != "afk" {filter_condition}      
        group by {group_by_cols_tw_str}
        union all
        SELECT {group_by_cols_tw_str}, concat({group_by_cols_tw_str}) ts, sum(time_diff(duration,"00:00:00",SECOND)) as data, {lag} lag             
        from calcium-sunbeam-341614.rtp_solution.{self.table_name}  
        where end_timestamp <= "{end_ts_lag}" and start_timestamp >= "{start_ts_lag}" 
        and source != "afk" {filter_condition}      
        group by {group_by_cols_tw_str}
        """

    def query_apps_total_duration(self, start_ts, end_ts, start_ts_lag, end_ts_lag, group_by_cols_tw_str, lag=1,
                         filter_condition=''):
        return f"""
        SELECT {group_by_cols_tw_str}, concat({group_by_cols_tw_str}) ts, sum(time_diff(duration,"00:00:00",SECOND)) as data, 0 lag
        from calcium-sunbeam-341614.rtp_solution.{self.table_name}  
        where end_timestamp <= "{end_ts}" and start_timestamp >= "{start_ts}" 
        {filter_condition}      
        group by {group_by_cols_tw_str}
        union all
        SELECT {group_by_cols_tw_str}, concat({group_by_cols_tw_str}) ts, sum(time_diff(duration,"00:00:00",SECOND)) as data, {lag} lag             
        from calcium-sunbeam-341614.rtp_solution.{self.table_name}  
        where end_timestamp <= "{end_ts_lag}" and start_timestamp >= "{start_ts_lag}" 
        {filter_condition}      
        group by {group_by_cols_tw_str}
        """

    def query_apps_top_10_by_dim_by_duration(self, start_ts, end_ts, group_by_dim_str, filter_condition=''):
        if group_by_dim_str is None:
            return f"""with T as(
            SELECT application_name,sum(time_diff(duration,"00:00:00",SECOND)) duration
            from calcium-sunbeam-341614.rtp_solution.{self.table_name}
            where
                start_timestamp >= "{start_ts}"
                and end_timestamp < "{end_ts}"
                and source != "afk" {filter_condition}
            group by 
                application_name
           )
           select array_agg(struct(application_name as name, round(duration) as value) order by duration desc limit 10) data from T
           """
        else:
            return f"""with T as(
            SELECT application_name,{group_by_dim_str},sum(time_diff(duration,"00:00:00",SECOND)) duration
            from calcium-sunbeam-341614.rtp_solution.{self.table_name}
            where
                start_timestamp >= "{start_ts}"
                and end_timestamp < "{end_ts}"
                and source != "afk" {filter_condition}
            group by 
                application_name, {group_by_dim_str}
           )
           select {group_by_dim_str}, array_agg(struct(application_name as name, round(duration) as value) order by duration desc limit 10) data from T
           group by {group_by_dim_str}
            """

    def query_domains_top_10_by_dim_by_duration(self, start_ts, end_ts, group_by_dim_str, filter_condition=''):
        if group_by_dim_str is None:
            return f"""with T as(
            SELECT browser_tab_domain,sum(time_diff(duration,"00:00:00",SECOND)) duration
            from calcium-sunbeam-341614.rtp_solution.{self.table_name}
            where
                start_timestamp >= "{start_ts}"
                and end_timestamp < "{end_ts}"
                and source = "browser" 
                and browser_tab_domain !='' {filter_condition}
            group by 
                browser_tab_domain
           )
           select array_agg(struct(browser_tab_domain as name, round(duration) as value) order by duration desc limit 10) data from T
           """
        else:
            return f"""with T as(
            SELECT browser_tab_domain,{group_by_dim_str},sum(time_diff(duration,"00:00:00",SECOND)) duration
            from calcium-sunbeam-341614.rtp_solution.{self.table_name}
            where
                start_timestamp >= "{start_ts}"
                and end_timestamp < "{end_ts}"
                and source = "browser" 
                and browser_tab_domain !='' {filter_condition}
            group by 
                browser_tab_domain, {group_by_dim_str}
           )
           select {group_by_dim_str}, array_agg(struct(browser_tab_domain as name, round(duration) as value) order by duration desc limit 10) data from T
           group by {group_by_dim_str}
            """

    def query_apps_domains_top_10_by_dim_by_duration(self, start_ts, end_ts, group_by_dim_str, filter_condition=''):
        if group_by_dim_str is None:
            return f"""with T as (
                SELECT application_name name,sum(time_diff(duration,"00:00:00",SECOND)) duration
                from calcium-sunbeam-341614.rtp_solution.{self.table_name}
                where
                    start_timestamp >= "{start_ts}"
                    and end_timestamp < "{end_ts}"
                    and source != "afk" {filter_condition}
                group by 
                    application_name
                union all
                SELECT browser_tab_domain name,sum(time_diff(duration,"00:00:00",SECOND)) duration
                from calcium-sunbeam-341614.rtp_solution.{self.table_name}
                where
                    start_timestamp >= "{start_ts}"
                    and end_timestamp < "{end_ts}"
                    and source = "browser" 
                    and browser_tab_domain !='' {filter_condition}
                group by 
                    browser_tab_domain
               )
               select array_agg(struct(name as name, round(duration) as value) order by duration desc limit 10) data from T
            """
        else:
            return f"""with T as(
                SELECT application_name name,{group_by_dim_str},sum(time_diff(duration,"00:00:00",SECOND)) duration
                from calcium-sunbeam-341614.rtp_solution.{self.table_name}
                where
                    start_timestamp >= "{start_ts}"
                    and end_timestamp < "{end_ts}"
                    and source != "afk" {filter_condition}
                group by 
                    application_name, {group_by_dim_str}
                union all
                SELECT browser_tab_domain name,{group_by_dim_str},sum(time_diff(duration,"00:00:00",SECOND)) duration
                from calcium-sunbeam-341614.rtp_solution.{self.table_name}
                where
                    start_timestamp >= "{start_ts}"
                    and end_timestamp < "{end_ts}"
                    and source = "browser" 
                    and browser_tab_domain !='' {filter_condition}
                group by 
                    browser_tab_domain, {group_by_dim_str}
                )
                select {group_by_dim_str}, array_agg(struct(name as name, round(duration) as value) order by duration desc limit 10) data from T
                group by {group_by_dim_str}
            """

    def get_groupby_data(self, start_ts, end_ts, start_ts_lag, end_ts_lag, group_by_dim_str, group_by_cols_tw_str,
                         lag, filter_condition):
        groupby_query = self.query_apps_groupby(start_ts, end_ts, start_ts_lag, end_ts_lag, group_by_dim_str,
                                                 group_by_cols_tw_str, lag, filter_condition)
        result = bq_client.query(groupby_query)
        return result

    def get_groupby_duration_data(self, start_ts, end_ts, start_ts_lag, end_ts_lag, group_by_dim_str, group_by_cols_tw_str,
                         lag, filter_condition):
        groupby_query = self.query_apps_duration_groupby(start_ts, end_ts, start_ts_lag, end_ts_lag, group_by_dim_str,
                                                 group_by_cols_tw_str, lag, filter_condition)
        result = bq_client.query(groupby_query)
        return result

    def get_total_active_duration_data(self, start_ts, end_ts, start_ts_lag, end_ts_lag, group_by_cols_tw_str, lag,
                       filter_condition):
        total_query = self.query_apps_total_active_duration(start_ts, end_ts, start_ts_lag, end_ts_lag,
                                            group_by_cols_tw_str, lag, filter_condition)
        result = bq_client.query(total_query)
        return result

    def get_total_duration_data(self, start_ts, end_ts, start_ts_lag, end_ts_lag, group_by_cols_tw_str, lag,
                       filter_condition):
        total_query = self.query_apps_total_duration(start_ts, end_ts, start_ts_lag, end_ts_lag,
                                            group_by_cols_tw_str, lag, filter_condition)
        result = bq_client.query(total_query)
        return result

    def get_top_10_apps_by_dim_by_duration_data(self, start_ts, end_ts, group_by_dim_str, filter_condition):
        top_10_query = self.query_apps_top_10_by_dim_by_duration(start_ts, end_ts, group_by_dim_str,
                                                                 filter_condition)
        result = bq_client.query(top_10_query)
        return result

    def get_top_10_domains_by_dim_by_duration_data(self, start_ts, end_ts, group_by_dim_str, filter_condition):
        top_10_query = self.query_domains_top_10_by_dim_by_duration(start_ts, end_ts, group_by_dim_str,
                                                                    filter_condition)
        result = bq_client.query(top_10_query)
        return result

    def get_top_10_apps_domains_by_dim_by_duration_data(self, start_ts, end_ts, group_by_dim_str, filter_condition):
        top_10_query = self.query_apps_domains_top_10_by_dim_by_duration(start_ts, end_ts, group_by_dim_str,
                                                                         filter_condition)
        result = bq_client.query(top_10_query)
        return result


log_decorate_class(AppsDAO)
