from concurrent.futures import as_completed

import pandas as pd
from datetime import datetime
from app.schemas.api_input import APIInputBase
from app.custom_logging.log_decorator import log_decorate_class
from app.utils.utils import Utils
from app.processor.apps import AppsProcessor



class AppsService:
    def __init__(self, executor=None):
        self.apps_processor = None
        self.executor = executor
        self.DATA_SUBSET = [
            {'name':"total_active_duration", 'query':"total_active_duration_query",'groupby_dimensions':[]},
            {"name": 'top_10_apps_domains_by_duration', "query": "top_10_apps_domains_by_dim_by_duration",
             "groupby_dimensions": []},
            {"name": 'top_10_apps_by_duration', "query": "top_10_apps_by_dim_by_duration","groupby_dimensions": []},
            {"name": 'top_10_domains_by_duration', "query": "top_10_domains_by_dim_by_duration","groupby_dimensions": []}
        ]
        self.ACTIVITY_SUBSET = [
            {'name': "total_duration", 'query': "total_duration_query", 'groupby_dimensions': []},
            {"name": "count_by_is_activity_type", "query": "groupby_query", "groupby_dimensions": ["source"]},
            {"name": "duration_sec_by_activity_type", "query": "groupby_duration_query",
             "groupby_dimensions": ["source"]},
            {"name": "count_by_is_active", "query": "groupby_query", "groupby_dimensions": ["is_active"]},
            {"name": "duration_sec_by_is_active", "query": "groupby_duration_query", "groupby_dimensions": ["is_active"]}
        ]

    def _get_response_body(self, name, duration, view, view_filters, start_date, end_date, company_id, user_id):
        return {"data": {
            "name": str(name),
            "duration": str(duration),
            # "source": "all",
            "view": str(view),
            "view_filters": view_filters,
            "start_timestamp": str(start_date),
            "end_timestamp": str(end_date),
            "company_id": company_id,
            "user_id": user_id,
            "data": []
        }
        }

    def get_apps_internet_data(self, request: APIInputBase):
        name = 'Application_Internet'
        time_window = request.time_window
        user_id = request.user_id
        company_id = request.company_id
        view = request.view
        view_filters = request.view_filters
        filter_condition = Utils.generate_filter_condition(company_id, view, view_filters)

        current_ts = pd.Timestamp(datetime.utcnow())
        self.apps_processor = AppsProcessor(time_window, current_ts, filter_condition)
        start_ts, end_ts = Utils.get_start_end_ts(current_ts, time_window)
        response_body= self._get_response_body(name, time_window, view, view_filters, start_ts, end_ts, company_id, user_id)

        threads = []
        data = []

        # executor = ThreadPoolExecutor(8)
        for kwargs in self.DATA_SUBSET:
            threads.append(self.executor.submit(self.get_data_by_query, **kwargs))

        for future in as_completed(threads):
            data.append(future.result())

        # Get response order as per DATA_SUBSET
        data_final = [list(filter(lambda x:x['type'] == i['name'], data))[0] for i in self.DATA_SUBSET]

        response_body['data']['data'] = data_final
        return response_body

    def get_activity_data(self, request: APIInputBase):
        name = 'Activity'
        time_window = request.time_window
        user_id = request.user_id
        company_id = request.company_id
        view = request.view
        view_filters = request.view_filters
        filter_condition = Utils.generate_filter_condition(company_id, view, view_filters)

        current_ts = pd.Timestamp(datetime.utcnow())
        self.apps_processor = AppsProcessor(time_window, current_ts, filter_condition)
        start_ts, end_ts = Utils.get_start_end_ts(current_ts, time_window)
        response_body = self._get_response_body(name, time_window, view, view_filters, start_ts, end_ts, company_id,user_id)
        threads = []
        data = []

        for kwargs in self.ACTIVITY_SUBSET:
            threads.append(self.executor.submit(self.get_data_by_query, **kwargs))

        # Add separate thread to get total working hours
        total_duration_thread = self.executor.submit(self.apps_processor.get_total_working_duration_for_all_emps)

        for future in as_completed(threads):
            data.append(future.result())

        if as_completed(total_duration_thread):
            total_duration = total_duration_thread.result()

        # Get response order as per DATA_SUBSET
        data_final = [list(filter(lambda x: x['type'] == i['name'], data))[0] for i in self.ACTIVITY_SUBSET]

        # Custom processing to get Offline duration
        # Offline = total_duration - active - inactive
        duration_data = list(filter(lambda x: x['type'] == 'duration_sec_by_is_active',data_final))[0]
        active_inactive_map = {i['is_active']: i['value'] for i in duration_data['series']}
        offline_duration = total_duration - active_inactive_map['Active'] - active_inactive_map['Inactive']
        data_final.append({'type':'pc_activity', 'series':[
            {'is_active': 'Active', 'value':active_inactive_map['Active']},
            {'is_active': 'Inactive', 'value': active_inactive_map['Inactive']},
            {'is_active':'Offline','value':str(offline_duration)}
            ]
        })

        response_body['data']['data'] = data_final
        return response_body

    def get_data_by_query(self, name, query, groupby_dimensions):
        print(f"START name:{name}, time:{pd.Timestamp(datetime.utcnow())}, 'groupby_dimensions':{groupby_dimensions}")
        if query == "total_duration_query":
            df = self.apps_processor.process_total_duration_query()
            data_series = df.apply(lambda x: Utils.dim_data_to_dict(x, groupby_dimensions), axis=1)
        elif query == "total_active_duration_query":
            df = self.apps_processor.process_total_active_duration_query()
            data_series = df.apply(lambda x: Utils.dim_data_to_dict(x, groupby_dimensions), axis=1)
        elif query == "top_10_apps_by_dim_by_duration":
            df = self.apps_processor.process_top_n_apps_by_dim_by_duration_query(groupby_dimensions)
            data_series = df.apply(lambda x: Utils.dim_data_to_dict_for_top_n(x, groupby_dimensions), axis=1)
        elif query == "top_10_domains_by_dim_by_duration":
            df = self.apps_processor.process_top_n_domains_by_dim_by_duration_query(groupby_dimensions)
            data_series = df.apply(lambda x: Utils.dim_data_to_dict_for_top_n(x, groupby_dimensions), axis=1)
        elif query == "top_10_apps_domains_by_dim_by_duration":
            df = self.apps_processor.process_top_n_apps_domains_by_dim_by_duration_query(groupby_dimensions)
            data_series = df.apply(lambda x: Utils.dim_data_to_dict_for_top_n(x, groupby_dimensions), axis=1)
        elif query == "groupby_query":
            df = self.apps_processor.process_groupby_query(groupby_dimensions)
            data_series = df.apply(lambda x: Utils.dim_data_to_dict(x, groupby_dimensions), axis=1)
        elif query == "groupby_duration_query":
            df = self.apps_processor.process_groupby_duration_query(groupby_dimensions)
            data_series = df.apply(lambda x: Utils.dim_data_to_dict(x, groupby_dimensions), axis=1)
        else:
            raise Exception('Provided query type does not exists')
        json_op = {
            "type": name,
            "series": list(data_series.values)
        }
        print(f"END name:{name}, time:{pd.Timestamp(datetime.utcnow())}")
        return json_op


log_decorate_class(AppsService)
