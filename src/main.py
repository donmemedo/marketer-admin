"""_summary_
"""
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from config import settings
from routers.marketer import marketer
from routers.factor import factor
from routers.user import user
from routers.database import database
from auth.permissions import permissions
from auth.registration import get_token, set_permissions

# from routers.subuser import subuser
from src.tools.logger import logger
from src.tools.database import get_database
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from khayyam import JalaliDatetime as jd


app = FastAPI(
    version=settings.VERSION,
    title=settings.SWAGGER_TITLE,
    docs_url="/docs",
    redoc_url="/redocs",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.ORIGINS.split(","),
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event("startup")
async def startup_events():
    get_database()
    token = await get_token()
    await set_permissions(permissions, token)


@app.get("/health-check", tags=["Deafult"])
def health_check():
    logger.info("Status of Marketer Admin Service is OK")
    return {"status": "OK"}


@app.get("/ip-getter", tags=["Deafult"])
async def read_root(request: Request):
    client_host = request.client.host
    client_scope = request.scope["client"]
    logger.info(f"client host is {client_host}")
    logger.info(f"client scope is {client_scope}")

    return {"client_host": client_host, "client_scope": client_scope}


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request, exc):
    response = {
        "result": [],
        "timeGenerated": jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
        "error": {
            # "message": "کمیسیون را وارد کنید.",
            "message": "ورودی‌ها را چک کرده و سپس به درستی وارد کنید.",
            "code": "30010",
        },
    }
    return JSONResponse(status_code=400, content=response)


# Add all routers
app.include_router(marketer, prefix="")
app.include_router(factor, prefix="")
app.include_router(user, prefix="")
app.include_router(database, prefix="")
# app.include_router(subuser, prefix="")
