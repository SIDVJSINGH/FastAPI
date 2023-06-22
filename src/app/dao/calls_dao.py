from . import bq_client
from app.custom_logging.log_decorator import log_decorate_class


class CallsDAO:
    def __init__(self):
        self.table_name = "calls_flat"
    def query_calls_groupby(self, start_ts, end_ts, start_ts_lag, end_ts_lag, group_by_dim_str,
                            group_by_cols_tw_str, lag=1, filter_condition=''):
        return f"""
        SELECT {group_by_dim_str}, {group_by_cols_tw_str}, concat({group_by_cols_tw_str}) ts, COUNT(1) as data, 0 lag             
        from calcium-sunbeam-341614.rtp_solution.{self.table_name}  
        where end_date <= "{end_ts}" and start_date >= "{start_ts}" {filter_condition}      
        group by {group_by_dim_str}, {group_by_cols_tw_str}
        union all
        SELECT {group_by_dim_str}, {group_by_cols_tw_str}, concat({group_by_cols_tw_str}) ts, COUNT(1) as data, {lag} lag             
        from calcium-sunbeam-341614.rtp_solution.{self.table_name}  
        where end_date <= "{end_ts_lag}" and start_date >= "{start_ts_lag}" {filter_condition}    
        group by {group_by_dim_str}, {group_by_cols_tw_str}
        """

    def query_calls_total(self, start_ts, end_ts, start_ts_lag, end_ts_lag, group_by_cols_tw_str, lag=1,
                          filter_condition=''):
        return f"""
        SELECT {group_by_cols_tw_str}, concat({group_by_cols_tw_str}) ts, COUNT(1) as data, 0 lag             
        from calcium-sunbeam-341614.rtp_solution.{self.table_name}  
        where end_date <= "{end_ts}" and start_date >= "{start_ts}" {filter_condition}      
        group by {group_by_cols_tw_str}
        union all
        SELECT {group_by_cols_tw_str}, concat({group_by_cols_tw_str}) ts, COUNT(1) as data, {lag} lag             
        from calcium-sunbeam-341614.rtp_solution.{self.table_name}  
        where end_date <= "{end_ts_lag}" and start_date >= "{start_ts_lag}" {filter_condition}      
        group by {group_by_cols_tw_str}
        """

    def query_calls_total_duration(self, start_ts, end_ts, start_ts_lag, end_ts_lag, group_by_cols_tw_str, lag=1,
                         filter_condition=''):
        return f"""
        SELECT {group_by_cols_tw_str}, concat({group_by_cols_tw_str}) ts, sum(time_diff(duration,"00:00:00",SECOND)) as data, 0 lag
        from calcium-sunbeam-341614.rtp_solution.{self.table_name}  
        where end_date <= "{end_ts}" and start_date >= "{start_ts}" 
        {filter_condition}      
        group by {group_by_cols_tw_str}
        union all
        SELECT {group_by_cols_tw_str}, concat({group_by_cols_tw_str}) ts, sum(time_diff(duration,"00:00:00",SECOND)) as data, {lag} lag             
        from calcium-sunbeam-341614.rtp_solution.{self.table_name}  
        where end_date <= "{end_ts_lag}" and start_date >= "{start_ts_lag}" 
        {filter_condition}      
        group by {group_by_cols_tw_str}
        """

    def query_calls_top_10_by_dim_by_count(self, start_ts, end_ts, group_by_dim_str,filter_condition=''):
        if group_by_dim_str is None:
            return f"""with T as(
                SELECT user_id,employee_id,employee_name,count(1) cnt
                from calcium-sunbeam-341614.rtp_solution.{self.table_name}
                where
                    start_date >= "{start_ts}"
                    and end_date <= "{end_ts}" {filter_condition}
                group by 
                    user_id,employee_id,employee_name
                )
                select array_agg(struct(user_id as email,employee_id as profile_id, employee_name as name, cnt as count) order by cnt desc limit 10) data from T
                """
        else:
            return f"""with T as(
                SELECT user_id,employee_id,employee_name,{group_by_dim_str},count(1) cnt
                from calcium-sunbeam-341614.rtp_solution.{self.table_name}
                where
                    start_date >= "{start_ts}"
                    and end_date <= "{end_ts}" {filter_condition}
                group by 
                    user_id,employee_id,employee_name, {group_by_dim_str}
                )
                select {group_by_dim_str}, array_agg(struct(user_id as email,employee_id as profile_id, employee_name as name, cnt as count) order by cnt desc limit 10) data from T
                group by {group_by_dim_str}
                """

    def query_calls_top_10_by_dim_by_duration(self, start_ts, end_ts, group_by_dim_str, filter_condition=''):
        if group_by_dim_str is None:
            return f"""with T as(
            SELECT user_id,employee_id,employee_name,sum(time_diff(duration,"00:00:00",SECOND)) duration
            from calcium-sunbeam-341614.rtp_solution.{self.table_name}
            where
                start_date >= "{start_ts}"
                and end_date < "{end_ts}" {filter_condition}
            group by 
                user_id,employee_id,employee_name
           )
           select array_agg(struct(user_id as email,employee_id as profile_id, employee_name as name, round(duration/60) as duration_minutes) order by duration desc limit 10) data from T
           """
        else:
            return f"""with T as(
            SELECT user_id,employee_id,employee_name,{group_by_dim_str},sum(time_diff(duration,"00:00:00",SECOND)) duration
            from calcium-sunbeam-341614.rtp_solution.{self.table_name}
            where
                start_date >= "{start_ts}"
                and end_date < "{end_ts}" {filter_condition}
            group by 
                user_id,employee_id,employee_name, {group_by_dim_str}
           )
           select {group_by_dim_str}, array_agg(struct(user_id as email,employee_id as profile_id, employee_name as name, round(duration/60) as duration_minutes) order by duration desc limit 10) data from T
           group by {group_by_dim_str}
            """

    def get_groupby_data(self, start_ts, end_ts, start_ts_lag, end_ts_lag, group_by_dim_str, group_by_cols_tw_str,
                               lag, filter_condition):
        groupby_query = self.query_calls_groupby(start_ts, end_ts, start_ts_lag, end_ts_lag, group_by_dim_str,
                                                       group_by_cols_tw_str, lag, filter_condition)
        result = bq_client.query(groupby_query)
        return result

    def get_total_data(self, start_ts, end_ts, start_ts_lag, end_ts_lag, group_by_cols_tw_str, lag,
                             filter_condition):
        total_query = self.query_calls_total(start_ts, end_ts, start_ts_lag, end_ts_lag,
                                                   group_by_cols_tw_str, lag, filter_condition)
        result = bq_client.query(total_query)
        return result

    def get_total_duration_data(self, start_ts, end_ts, start_ts_lag, end_ts_lag, group_by_cols_tw_str, lag,
                             filter_condition):
        total_query = self.query_calls_total_duration(start_ts, end_ts, start_ts_lag, end_ts_lag,
                                                   group_by_cols_tw_str, lag, filter_condition)
        result = bq_client.query(total_query)
        return result

    def get_top_10_by_dim_by_count_data(self, start_ts, end_ts, group_by_dim_str, filter_condition):
        top_10_query = self.query_calls_top_10_by_dim_by_count(start_ts, end_ts, group_by_dim_str,
                                                                     filter_condition)
        result = bq_client.query(top_10_query)
        return result

    def get_top_10_by_dim_by_duration_data(self, start_ts, end_ts, group_by_dim_str, filter_condition):
        top_10_query = self.query_calls_top_10_by_dim_by_duration(start_ts, end_ts, group_by_dim_str,
                                                                        filter_condition)
        result = bq_client.query(top_10_query)
        return result


log_decorate_class(CallsDAO)
