from concurrent.futures import as_completed

import pandas as pd
from datetime import datetime
from app.schemas.api_input import APIInputBase
from app.custom_logging.log_decorator import log_decorate_class
from app.utils.utils import Utils
from app.processor.messages import MessagesProcessor


class MessagesService:
    def __init__(self, executor=None):
        self.messages_processor = None
        self.executor = executor
        self.DATA_SUBSET = [
            {'name':"total", 'query':"total_query",'groupby_dimensions':[]},
            {"name":"by_inbound_outbound","query": "groupby_query", "groupby_dimensions": ["inbound_outbound"]},
            {"name":"by_type","query": "groupby_query", "groupby_dimensions": ["type"]},
            {"name":"by_inbound_outbound_by_type","query": "groupby_query", "groupby_dimensions": ["inbound_outbound", "type"]},
            {"name":'top_10_by_inbound_outbound_by_count',"query":"top_10_by_dim_by_count","groupby_dimensions":["inbound_outbound"]},
            {"name":'top_10_by_count',"query":"top_10_by_dim_by_count","groupby_dimensions":[]}
        ]

    def _get_response_body(self,duration, view, view_filters,start_date, end_date, company_id, user_id):
        return {"data": {
            "name": "Messages",
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
        self.messages_processor = MessagesProcessor(time_window, current_ts, filter_condition)
        start_ts, end_ts = Utils.get_start_end_ts(current_ts, time_window)
        response_body = self._get_response_body(time_window, view, view_filters, start_ts, end_ts, company_id,user_id)

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

    def get_data_by_query(self, name, query, groupby_dimensions):
        # print(f"START name:{name}, time:{pd.Timestamp(datetime.utcnow())}, 'groupby_dimensions':{groupby_dimensions}")
        if query == "groupby_query":
            df = self.messages_processor.process_groupby_query(groupby_dimensions)
            data_series = df.apply(lambda x: Utils.dim_data_to_dict(x, groupby_dimensions), axis=1)
        elif query == "total_query":
            df = self.messages_processor.process_total_query()
            data_series = df.apply(lambda x: Utils.dim_data_to_dict(x, groupby_dimensions), axis=1)
        elif query == "top_10_by_dim_by_count":
            df = self.messages_processor.process_top_n_by_dim_by_count_query(groupby_dimensions)
            data_series = df.apply(lambda x: Utils.dim_data_to_dict_for_top_n(x, groupby_dimensions), axis=1)
        else:
            raise Exception('Provided query type does not exists')
        json_op = {
            "type": name,
            "series": list(data_series.values)
        }
        # print(f"END name:{name}, time:{pd.Timestamp(datetime.utcnow())}")
        return json_op


log_decorate_class(MessagesService)
