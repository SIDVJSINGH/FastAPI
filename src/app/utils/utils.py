import pandas as pd
from dateutil.relativedelta import relativedelta
from datetime import datetime

from app.utils.constants import TimeWindow
from app.custom_logging.log_decorator import log_decorate_class
from app.utils.constants import View


class Utils:
    @staticmethod
    def is_time_window_valid(time_window):
        return TimeWindow.has_value(time_window)

    @staticmethod
    def get_start_end_ts(end_ts, time_window):
        if not Utils.is_time_window_valid(time_window):
            raise Exception('Invalid Time Window')
        end_ts = pd.Timestamp(end_ts)
        end_ts_date = pd.Timestamp(end_ts.date())
        start_ts = None
        if time_window == 'yesterday':
            start_ts = end_ts_date - relativedelta(days=1)
            end_ts = end_ts_date
        elif time_window == 'week':
            start_ts = end_ts_date - relativedelta(weeks=1) + relativedelta(days=1)
        elif time_window == 'month':
            start_ts = end_ts_date - relativedelta(months=1) + relativedelta(days=1)
        elif time_window == 'quarter':
            start_ts = end_ts_date - relativedelta(months=3) + relativedelta(days=1)
        elif time_window == '6-month':
            start_ts = end_ts_date - relativedelta(months=6) + relativedelta(days=1)
        elif time_window == '9-month':
            start_ts = end_ts_date - relativedelta(months=9) + relativedelta(days=1)
        elif time_window == 'annual' or time_window == '12-month':
            start_ts = end_ts_date - relativedelta(years=1) + relativedelta(days=1)
        elif time_window == 'all':
            start_ts = None
        return start_ts, end_ts

    @staticmethod
    def get_join_string(group_by_dim):
        join_string = f'ON T1.{group_by_dim[0]}=T2.{group_by_dim[0]} '
        for dim in group_by_dim[1:]:
            join_string = join_string + f'AND T1.{dim} = T2.{dim} '
        return join_string

    @staticmethod
    def one_dim_data_to_dict(df, dim):
        return {
            "name": df[dim],
            "timestamp": list(map(lambda x: x.strftime('%Y-%m-%dT%H:%M:%S'), df['timestamp'])),
            "data": df['data'],
            "value": df['value'],
            "value_lag_1": df['value_lag_1'],
            "change": df['change']
        }

    @staticmethod
    def dim_data_to_dict(x, dim_list):
        dim_body = {dim: x.get(dim, None) for dim in dim_list}
        data_body = {
            "timestamp": list(map(lambda x: x.strftime('%Y-%m-%dT%H:%M:%S'), x.get('timestamp', pd.Timestamp(datetime.utcnow())))),
            "data": x.get('data'),
            "value": x.get('value'),
            "value_lag_1": x.get('value_lag_1'),
            "change": x.get('change')
        }
        return {**dim_body, **data_body}

    @staticmethod
    def dim_data_to_dict_for_top_n(x, dim_list):
        dim_body = {dim: x.get(dim) for dim in dim_list}
        data_body = {
            "data": x.get('data')
        }
        return {**dim_body, **data_body}

    @staticmethod
    def generate_filter_condition(company_id, view, view_filters):
        static_condition = f""" and company_id={company_id}"""
        view_condition = View.generate_view_filter_condition(view, view_filters)
        if view_condition is not None:
            return static_condition + " and " + view_condition
        return static_condition


log_decorate_class(Utils)
