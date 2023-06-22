import asyncio
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd
from datetime import datetime
from app.schemas.api_input import APIInputBase
from app.custom_logging.log_decorator import log_decorate_class
from app.utils.utils import Utils
from app.processor.calls import CallsProcessor


class CallsService:

    def __init__(self, executor=None):
        self.calls_processor = None
        self.executor = executor
        self.DATA_SUBSET = [
            {'name':"total", 'query':"total_query",'groupby_dimensions':[]},
            {'name': "total_duration", 'query': "total_duration_query", 'groupby_dimensions': []},
            {"name":"by_inbound_outbound","query": "groupby_query", "groupby_dimensions": ["inbound_outbound"]},
            {"name":"by_call_type","query": "groupby_query", "groupby_dimensions": ["call_type"]},
            {"name":"by_inbound_outbound_by_call_type","query": "groupby_query", "groupby_dimensions": ["inbound_outbound", "call_type"]},
            {"name":'top_10_by_inbound_outbound_by_count',"query":"top_10_by_dim_by_count","groupby_dimensions":["inbound_outbound"]},
            {"name":'top_10_by_inbound_outbound_by_duration',"query":"top_10_by_dim_by_duration","groupby_dimensions":["inbound_outbound"]}
        ]

    def _get_response_body(self, start_date, end_date, duration, view_filters, view,company_id, user_id):

        return {"data": {
            "name": "Calls",
            "duration": str(duration),
             # "source": "all",
            "view": str(view),
            "view_filters": view_filters,
            "start_timestamp": str(start_date),
            "end_timestamp": str(end_date),
            "company_id" : company_id,
            "user_id": user_id,
            "data": []
        }
        }

    def get_all_data(self, request: APIInputBase):
        time_window = request.time_window

        user_id = request.user_id
        company_id = request.company_id
        view = request.view
        view_filters = request.view_filters
        filter_condition = Utils.generate_filter_condition(company_id, view, view_filters)

        current_ts = pd.Timestamp(datetime.utcnow())
        self.calls_processor = CallsProcessor(time_window, current_ts, filter_condition)
        start_ts, end_ts = Utils.get_start_end_ts(current_ts, time_window)
        response_body = self._get_response_body(start_ts, end_ts, time_window, view_filters, view, company_id, user_id)

        threads = []
        data = []

        # executor = ThreadPoolExecutor(8)
        for kwargs in self.DATA_SUBSET:
            threads.append(self.executor.submit(self.get_data_by_query, **kwargs))

        for future in as_completed(threads):
            data.append(future.result())

        # Get response order as per DATA_SUBSET
        data_final = [list(filter(lambda x: x['type'] == i['name'], data))[0] for i in self.DATA_SUBSET]

        response_body['data']['data'] = data_final
        return response_body

    def get_data_by_query(self, name, query, groupby_dimensions):

        # print(f"START name:{name}, time:{pd.Timestamp(datetime.utcnow())}, 'groupby_dimensions':{groupby_dimensions}")
        if query == "groupby_query":
            df = self.calls_processor.process_groupby_query(groupby_dimensions)
            data_series = df.apply(lambda x: Utils.dim_data_to_dict(x, groupby_dimensions), axis=1)
        elif query == "total_query":
            df = self.calls_processor.process_total_query()
            data_series = df.apply(lambda x: Utils.dim_data_to_dict(x, groupby_dimensions), axis=1)
        elif query == "total_duration_query":
            df = self.calls_processor.process_total_duration_query()
            data_series = df.apply(lambda x: Utils.dim_data_to_dict(x, groupby_dimensions), axis=1)
        elif query == "top_10_by_dim_by_count":
            df = self.calls_processor.process_top_n_by_dim_by_count_query(groupby_dimensions)
            data_series = df.apply(lambda x: Utils.dim_data_to_dict_for_top_n(x, groupby_dimensions), axis=1)
        elif query == "top_10_by_dim_by_duration":
            df = self.calls_processor.process_top_n_by_dim_by_duration_query(groupby_dimensions)
            data_series = df.apply(lambda x: Utils.dim_data_to_dict_for_top_n(x, groupby_dimensions), axis=1)
        else:
            raise Exception('Provided query type does not exists')
        json_op = {
            "type": name,
            "series": list(data_series.values)
        }
        # print(f"END name:{name}, time:{pd.Timestamp(datetime.utcnow())}")
        return json_op




log_decorate_class(CallsService)
