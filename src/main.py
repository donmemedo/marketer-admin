"""_summary_
"""
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from config import settings
from routers.marketer import marketer
from routers.factor import factor
from routers.user import user
from routers.subuser import subuser
from src.tools.logger import logger
from src.tools.database import get_database
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from khayyam import JalaliDatetime as jd


app = FastAPI(version=settings.VERSION, title=settings.SWAGGER_TITLE)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event("startup")
async def startup_events():
    get_database()

@app.get("/health-check", tags=["Deafult"])
def health_check():
    logger.info("Status of Marketer Admin Service is OK")
    return {"status": "OK"}


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request, exc):
    zizo= {
        "result": [],
        "timeGenerated": jd.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
        "error": {
            "errormessage": "کمیسیون را وارد کنید.",
            "errorcode": "30010"
        }
    }
    return JSONResponse(status_code=422, content=zizo)


# Add all routers
app.include_router(marketer, prefix="")
app.include_router(factor, prefix="")
app.include_router(user, prefix="")
app.include_router(subuser, prefix="")
