from . import bq_client
from app.custom_logging.log_decorator import log_decorate_class


class MessagesDAO:
    def __init__(self):
        self.table_name = "messages_flat"
    def query_messages_groupby(self, start_ts, end_ts, start_ts_lag, end_ts_lag, group_by_dim_str,
                            group_by_cols_tw_str, lag=1, filter_condition=''):
        return f"""
        SELECT {group_by_dim_str}, {group_by_cols_tw_str}, concat({group_by_cols_tw_str}) ts, COUNT(1) as data, 0 lag             
        from calcium-sunbeam-341614.rtp_solution.{self.table_name}  
        where message_timestamp <= "{end_ts}" and message_timestamp >= "{start_ts}" {filter_condition}      
        group by {group_by_dim_str}, {group_by_cols_tw_str}
        union all
        SELECT {group_by_dim_str}, {group_by_cols_tw_str}, concat({group_by_cols_tw_str}) ts, COUNT(1) as data, {lag} lag             
        from calcium-sunbeam-341614.rtp_solution.{self.table_name}  
        where message_timestamp <= "{end_ts_lag}" and message_timestamp >= "{start_ts_lag}" {filter_condition}    
        group by {group_by_dim_str}, {group_by_cols_tw_str}
        """

    def query_messages_total(self, start_ts, end_ts, start_ts_lag, end_ts_lag, group_by_cols_tw_str, lag=1,
                          filter_condition=''):
        return f"""
        SELECT {group_by_cols_tw_str}, concat({group_by_cols_tw_str}) ts, COUNT(1) as data, 0 lag             
        from calcium-sunbeam-341614.rtp_solution.{self.table_name}  
        where message_timestamp <= "{end_ts}" and message_timestamp >= "{start_ts}" {filter_condition}      
        group by {group_by_cols_tw_str}
        union all
        SELECT {group_by_cols_tw_str}, concat({group_by_cols_tw_str}) ts, COUNT(1) as data, {lag} lag             
        from calcium-sunbeam-341614.rtp_solution.{self.table_name}  
        where message_timestamp <= "{end_ts_lag}" and message_timestamp >= "{start_ts_lag}" {filter_condition}      
        group by {group_by_cols_tw_str}
        """

    def query_messages_top_10_by_dim_by_count(self, start_ts, end_ts, group_by_dim_str, filter_condition=''):
        if group_by_dim_str is None:
            return f"""with T as(
                        SELECT user_id,employee_id,employee_name,count(1) cnt
                        from calcium-sunbeam-341614.rtp_solution.{self.table_name}
                        where
                            message_timestamp >= "{start_ts}"
                            and message_timestamp <= "{end_ts}" {filter_condition}
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
                            message_timestamp >= "{start_ts}"
                            and message_timestamp <= "{end_ts}" {filter_condition}
                        group by 
                            user_id,employee_id,employee_name, {group_by_dim_str}
                        )
                        select {group_by_dim_str}, array_agg(struct(user_id as email,employee_id as profile_id, employee_name as name, cnt as count) order by cnt desc limit 10) data from T
                        group by {group_by_dim_str}
                        """

    def get_groupby_data(self, start_ts, end_ts, start_ts_lag, end_ts_lag, group_by_dim_str, group_by_cols_tw_str,
                               lag, filter_condition):
        groupby_query = self.query_messages_groupby(start_ts, end_ts, start_ts_lag, end_ts_lag, group_by_dim_str,
                                                       group_by_cols_tw_str, lag, filter_condition)
        result = bq_client.query(groupby_query)
        return result

    def get_total_data(self, start_ts, end_ts, start_ts_lag, end_ts_lag, group_by_cols_tw_str, lag,
                             filter_condition):
        total_query = self.query_messages_total(start_ts, end_ts, start_ts_lag, end_ts_lag,
                                                   group_by_cols_tw_str, lag, filter_condition)
        result = bq_client.query(total_query)
        return result

    def get_top_10_by_dim_by_count_data(self, start_ts, end_ts, group_by_dim_str, filter_condition):
        top_10_query = self.query_messages_top_10_by_dim_by_count(start_ts, end_ts, group_by_dim_str,
                                                                     filter_condition)
        result = bq_client.query(top_10_query)
        return result


log_decorate_class(MessagesDAO)
