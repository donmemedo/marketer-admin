"""_summary_
"""
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from config import settings
from routers.marketer import marketer
from routers.factor import factor
from routers.user import user
from routers.subuser import subuser


app = FastAPI(version=settings.VERSION, title=settings.SWAGGER_TITLE)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Add all routers
app.include_router(marketer, prefix="")
app.include_router(factor, prefix="")
app.include_router(user, prefix="")
app.include_router(subuser, prefix="")
