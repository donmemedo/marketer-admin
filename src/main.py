from fastapi import FastAPI
from config import settings
from fastapi.middleware.cors import CORSMiddleware
from routers.marketer import marketer
from routers.user import user_router
from routers.sub_user import sub_user_router


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
app.include_router(user_router, prefix="")
app.include_router(sub_user_router, prefix="")
