"""_summary_
"""
from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from khayyam import JalaliDatetime as jd

from auth.permissions import permissions
from auth.registration import get_token, set_permissions
from config import settings
from routers.client_marketer import client_marketer
from routers.client_user import client_user
from routers.client_volume_and_fee import client_volume_and_fee
from routers.database import database
from routers.factor import factor
from routers.factor_marketer_contract import marketer_contract
from routers.factor_marketer_contract_coefficient import marketer_contract_coefficient
from routers.factor_marketer_contract_deduction import marketer_contract_deduction
from routers.factor_marketer_ref_code import marketer_ref_code
from routers.factors import factors
from routers.grpc_services import grpc_services
from routers.marketer import marketer, marketer_relation
from routers.user import user
from routers.tbs_data_crawler import tbs_data_crawler
from src.tools.database import get_database
from src.tools.errors import get_error

# from routers.subuser import subuser
from src.tools.logger import logger

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
    status = 400
    try:
        err = get_error(exc.errors().__name__, exc.body["code"])
        status = exc.body["status"]
    except:
        for e in exc.errors():
            try:
                err = get_error(e["type"], e["ctx"]["error"])
                status = err["code"]
            except:
                err = get_error(e["type"], e["msg"])
                status = err["code"]
    response = {
        "result": [],
        "timeGenerated": jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
        "error": err,
    }
    logger.error(exc)
    return JSONResponse(status_code=status, content=response)


# Add all routers
app.include_router(marketer, prefix="")
app.include_router(marketer_relation, prefix="")
app.include_router(client_marketer, prefix="")
app.include_router(client_user, prefix="")
app.include_router(client_volume_and_fee, prefix="")
app.include_router(factor, prefix="")
app.include_router(marketer_ref_code, prefix="")
app.include_router(marketer_contract, prefix="")
app.include_router(marketer_contract_coefficient, prefix="")
app.include_router(marketer_contract_deduction, prefix="")
app.include_router(user, prefix="")
app.include_router(database, prefix="")
app.include_router(factors, prefix="")
app.include_router(grpc_services, prefix="")
app.include_router(tbs_data_crawler, prefix="")
# app.include_router(subuser, prefix="")
