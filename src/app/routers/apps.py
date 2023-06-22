from fastapi import APIRouter, Depends, status
from app.schemas.api_input import APIInputBase
from app.services.apps import AppsService
from . import executor

router = APIRouter(
    prefix='/apps',
    tags=['app'],
    # dependencies=[Depends(get_current_user)]
)

apps_service = AppsService(executor)


@router.post('/apps_internet/', status_code=status.HTTP_200_OK)
def fetch_apps_data(request: APIInputBase):
    response = apps_service.get_apps_internet_data(request)
    return response


@router.post('/activity/', status_code=status.HTTP_200_OK)
def fetch_apps_data(request: APIInputBase):
    response = apps_service.get_activity_data(request)
    return response
