from . import bq_client
from app.custom_logging.log_decorator import log_decorate_class


class EmployeesDAO:
    def __init__(self):
        self.table_name = "employees"

    def query_employee_count(self, filter_condition=''):
        # 1=1 filter added as dummy filter to introduct WHERE keyword,
        # as filter_condition string does not contain WHERE keyword
        return f"""
        SELECT distinct count(*) as emp_count FROM calcium-sunbeam-341614.rtp_solutions_staging.{self.table_name}
        where 1=1 {filter_condition}
        """


    def get_employee_count_data(self, filter_condition):
        query = self.query_employee_count(filter_condition)
        result = bq_client.query(query)
        return result


log_decorate_class(EmployeesDAO)
