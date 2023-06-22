from app.custom_logging.log_decorator import log_decorate_class

class TimeWindow():
    yesterday = 1
    week = 2
    month = 3
    quarter = 4
    six_month = 5
    nine_month = 6
    annual = 7
    all = 8

    _groupby_dict = {
        "yesterday": ["date_skey", "hour"],
        "week": ['date_skey'],
        "month": ["date_skey"],
        "quarter": ["year", "month"],
        "six_month": ["year", "month"],
        "6-month": ["year", "month"],
        "nine_month": ["year", "month"],
        "9-month": ["year", "month"],
        "annual": ["year", "month"],
        "12-month": ["year", "month"]
    }

    _timeformat = {
        "yesterday": "%Y%m%d%H",
        "week": "%Y%m%d",
        "month": "%Y%m%d",
        "quarter": "%Y%m",
        "six_month": "%Y%m",
        "6-month": "%Y%m",
        "nine_month": "%Y%m",
        "9-month": "%Y%m",
        "annual": "%Y%m",
        "12-month": "%Y%m"
    }

    _freq = {
        "yesterday": "H",
        "week": "D",
        "month": "D",
        "quarter": "M",
        "six_month": "M",
        "6-month": "M",
        "nine_month": "M",
        "9-month": "M",
        "annual": "M",
        "12-month": "M"
    }

    @classmethod
    def has_value(cls, value):
        if value == '6-month':
            value = 'six_month'
        elif value == '9-month':
            value = 'nine_month'
        elif value == '12-month':
            value = 'annual'
        return hasattr(cls, value)

    @classmethod
    def get_groupby_cols(cls, time_window):
        return cls._groupby_dict[time_window]

    @classmethod
    def get_ts_format(cls, time_window):
        return cls._timeformat[time_window]

    @classmethod
    def get_date_range_freq(cls, time_window):
        return cls._freq[time_window]


class View():
    individual = 1
    team = 2
    manager = 3
    department = 4
    area = 5
    country = 6
    all = 7

    @classmethod
    def has_value(cls, value):
        return hasattr(cls, value)

    @classmethod
    def generate_view_filter_condition(cls, view, view_filters):
        if not View.has_value(view):
            raise Exception("Wrong value provided for View: {view}")
        view_condition = None
        # convert list to tuple. Tuple typecast doesn't work for list with single item
        view_filters = '('+','.join(view_filters) + ')'
        if view == 'all':
            view_condition = None
        elif view == 'individual':
            view_condition = f"""user_id in {view_filters}"""
        elif view == 'team':
            view_condition = f"""team_id in {view_filters}"""
        elif view == 'department':
            view_condition = f"""department_id in {view_filters}"""
        elif view == 'area':
            view_condition = f"""location_id in {view_filters}"""
        return view_condition


class DimensionValues:
    _calls_dim_values = {
        'inbound_outbound': ('Incoming', 'Outgoing'),
        'call_type': ('Known', 'Unknown', 'Internal')
    }
    _messages_dim_values = {
        'inbound_outbound': ('Incoming', 'Outgoing'),
        'type': ('Known', 'Unknown', 'Internal')
    }
    _emails_dim_values = {
        'inbound_outbound': ('inbound', 'outbound'),
        'type': ('Known', 'Unknown', 'Internal')
    }

    _apps_dim_values = {
        'source': ('application','browser','afk'),
        'is_active': ('Active', 'Inactive')
    }

    @classmethod
    def get_dimension_values(cls, module, dim):
        if module == 'calls':
            return cls._calls_dim_values[dim]
        elif module == 'messages':
            return cls._messages_dim_values[dim]
        elif module == 'emails':
            return cls._emails_dim_values[dim]
        elif module == 'apps':
            return cls._apps_dim_values[dim]
        else:
            raise Exception('Invalid module name passed to get dimension values.')


log_decorate_class(TimeWindow)
log_decorate_class(View)
log_decorate_class(DimensionValues)