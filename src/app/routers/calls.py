from fastapi import APIRouter, Depends, status
from app.schemas.api_input import APIInputBase
from app.services.calls import CallsService
from . import executor

router = APIRouter(
    prefix='/calls',
    tags=['calls'],
    # dependencies=[Depends(get_current_user)]
)

# executor = ThreadPoolExecutor(8)

calls_service = CallsService(executor)

@router.post('/', status_code=status.HTTP_200_OK)
def fetch_calls_data(request: APIInputBase):
    response = calls_service.get_all_data(request)
    return response
