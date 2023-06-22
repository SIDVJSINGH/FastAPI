from datetime import datetime
from dateutil.relativedelta import relativedelta
import pandas as pd
import numpy as np
from datetime import timedelta

from app.custom_logging.log_decorator import log_decorate_class
from app.dao.calls_dao import CallsDAO
from app.utils.constants import TimeWindow, DimensionValues
from app.utils.utils import Utils

calls_dao = CallsDAO()
MODULE = 'calls'


class CallsProcessor():
    def __init__(self, time_window, current_ts, filter_condition):
        self.time_window = time_window
        self.start_ts, self.end_ts = Utils.get_start_end_ts(current_ts, time_window)
        end_ts_lag = self.start_ts - relativedelta(seconds=1)
        self.start_ts_lag, self.end_ts_lag = Utils.get_start_end_ts(end_ts_lag, time_window)
        self.lag = 1
        self.filter_condition = filter_condition
        self.date_range = pd.period_range(self.start_ts, self.end_ts, freq=TimeWindow.get_date_range_freq(time_window))
        self.date_range = list(map(lambda x: x.to_timestamp(), self.date_range))

        self.ts_format = TimeWindow.get_ts_format(time_window)
        self.group_by_cols_tw = TimeWindow.get_groupby_cols(time_window)
        self.group_by_cols_tw_str = ','.join(self.group_by_cols_tw)

    def process_groupby_query(self, groupby_dimensions):
        group_by_dim_str = ','.join(groupby_dimensions)
        query_result = calls_dao.get_groupby_data(self.start_ts, self.end_ts, self.start_ts_lag, self.end_ts_lag,
                                                        group_by_dim_str,
                                                        self.group_by_cols_tw_str, self.lag, self.filter_condition)
        result_list = [dict(row) for row in query_result]
        df = pd.DataFrame(result_list)
        if df.shape[0] == 0:
            return df
        ts_format = TimeWindow.get_ts_format(self.time_window)
        df['timestamp'] = df['ts'].apply(lambda x: datetime.strptime(str(x), ts_format))

        df_lag = df[df['lag'] == 1]
        df = df[df['lag'] == 0]

        # fill missing records with 0
        idx = pd.MultiIndex.from_product(
            list(DimensionValues.get_dimension_values(MODULE,i.lower()) for i in groupby_dimensions) + [self.date_range],
            names=groupby_dimensions + ['timestamp'])
        df = df.set_index(groupby_dimensions + ['timestamp'])['data'].reindex(idx, fill_value=0).reset_index()

        df = df.groupby(groupby_dimensions)[['data', 'timestamp']].agg(list).reset_index()
        df['value'] = df['data'].apply(sum)

        df_lag = df_lag.groupby(groupby_dimensions)['data'].sum().reset_index().rename(
            columns={'data': 'value_lag_1'})
        df = df.merge(df_lag, how='left', on=groupby_dimensions)
        df['value_lag_1'].fillna(0, inplace=True)
        df['change'] = round((df['value'] - df['value_lag_1']) / df['value_lag_1'] * 100)
        df['change'].fillna(0, inplace=True)
        df.replace([np.inf, -np.inf], 0, inplace=True)
        return df

    def process_total_query(self):
        query_result = calls_dao.get_total_data(self.start_ts, self.end_ts, self.start_ts_lag, self.end_ts_lag,
                                                      self.group_by_cols_tw_str, self.lag, self.filter_condition)
        result_list = [dict(row) for row in query_result]
        df = pd.DataFrame(result_list)
        if df.shape[0] == 0:
            return df
        ts_format = TimeWindow.get_ts_format(self.time_window)
        df['timestamp'] = df['ts'].apply(lambda x: datetime.strptime(str(x), ts_format))

        df_lag = df[df['lag'] == 1]
        df = df[df['lag'] == 0]

        idx = self.date_range
        df = df.set_index('timestamp')['data'].reindex(idx, fill_value=0).reset_index()
        df = df.groupby(lambda x: True)[['data', 'timestamp']].agg(list).reset_index()
        df['value'] = df['data'].apply(sum)
        df['value_lag_1'] = df_lag['data'].sum()
        df['value_lag_1'].fillna(0, inplace=True)
        df['change'] = round((df['value'] - df['value_lag_1']) / df['value_lag_1'] * 100)
        df['change'].fillna(0, inplace=True)
        df.replace([np.inf, -np.inf], 0, inplace=True)
        return df

    def process_total_duration_query(self):
        query_result = calls_dao.get_total_duration_data(self.start_ts, self.end_ts, self.start_ts_lag, self.end_ts_lag,
                                                      self.group_by_cols_tw_str, self.lag, self.filter_condition)
        result_list = [dict(row) for row in query_result]
        df = pd.DataFrame(result_list)
        if df.shape[0] == 0:
            return df
        ts_format = TimeWindow.get_ts_format(self.time_window)
        df['timestamp'] = df['ts'].apply(lambda x: datetime.strptime(str(x), ts_format))

        df_lag = df[df['lag'] == 1]
        df = df[df['lag'] == 0]

        idx = self.date_range
        df = df.set_index('timestamp')['data'].reindex(idx, fill_value=0).reset_index()
        df = df.groupby(lambda x: True)[['data', 'timestamp']].agg(list).reset_index()
        df['value'] = df['data'].apply(sum)
        df['value_lag_1'] = df_lag['data'].sum()
        df['value_lag_1'].fillna(0, inplace=True)
        df['change'] = round((df['value'] - df['value_lag_1']) / df['value_lag_1'] * 100)
        df['change'].fillna(0, inplace=True)
        df.replace([np.inf, -np.inf], 0, inplace=True)
        # Convert seconds to string format like 2 Days 10:15:30
        df['value'] = str(timedelta(seconds=int(df['value'])))
        df['value_lag_1'] = str(timedelta(seconds=int(df['value_lag_1'])))
        return df

    def process_top_n_by_dim_by_count_query(self, groupby_dimensions):
        if groupby_dimensions == []:
            group_by_dim_str = None
        else:
            group_by_dim_str = ','.join(groupby_dimensions)
        query_result = calls_dao.get_top_10_by_dim_by_count_data(self.start_ts, self.end_ts,
                                                                       group_by_dim_str, self.filter_condition)
        result_list = [dict(row) for row in query_result]
        df = pd.DataFrame(result_list)
        return df

    def process_top_n_by_dim_by_duration_query(self, groupby_dimensions):
        if groupby_dimensions == []:
            group_by_dim_str = None
        else:
            group_by_dim_str = ','.join(groupby_dimensions)
        query_result = calls_dao.get_top_10_by_dim_by_duration_data(self.start_ts, self.end_ts,
                                                                          group_by_dim_str, self.filter_condition)
        result_list = [dict(row) for row in query_result]
        df = pd.DataFrame(result_list)
        return df


log_decorate_class(CallsProcessor)