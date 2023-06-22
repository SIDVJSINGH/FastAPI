from fastapi import FastAPI
from typing import Optional
from pydantic import BaseModel
import uvicorn
from app.routers import calls, messages, emails, apps

app = FastAPI()

# Swagger UI: http://127.0.0.1:8000/docs
# Redoc: http://127.0.0.1:8000/redoc

# This will create all tables based on models
# models.Base.metadata.create_all(database.engine)

app.include_router(calls.router)
app.include_router(messages.router)
app.include_router(emails.router)
app.include_router(apps.router)

if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000)
