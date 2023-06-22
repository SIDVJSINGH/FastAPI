from fastapi import APIRouter, Depends, status
from app.schemas.api_input import APIInputBase
from app.services.emails import EmailsService
from . import executor

router = APIRouter(
    prefix='/emails',
    tags=['emails'],
    # dependencies=[Depends(get_current_user)]
)

emails_service = EmailsService(executor)


@router.post('/', status_code=status.HTTP_200_OK)
def fetch_emails_data(request: APIInputBase):

    response = emails_service.get_all_data(request)
    return response
