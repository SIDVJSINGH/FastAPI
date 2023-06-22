from pydantic import BaseModel
from typing import List


class APIInputBase(BaseModel):
    time_window: str
    view: str
    view_filters: List[str]
    user_id: str
    company_id: int
