from fastapi import APIRouter, Depends, status
from app.schemas.api_input import APIInputBase
from app.services.messages import MessagesService
from . import executor

router = APIRouter(
    prefix='/messages',
    tags=['messages'],
    # dependencies=[Depends(get_current_user)]
)

messages_service = MessagesService(executor)


@router.post('/', status_code=status.HTTP_200_OK)
def fetch_messages_data(request: APIInputBase):

    response = messages_service.get_all_data(request)
    return response
