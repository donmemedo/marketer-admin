from functools import wraps

from fastapi import HTTPException
from src.auth.permission_enum import Service
from src.tools.logger import logger
from src.tools.tokens import get_role_permission


def authorize(permissions):
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            # if kwargs.get("request"):
            if kwargs.get("role_perm").get("roles"):  # (Service.MarketerAdmin.name):
                if not any(
                    [
                        # p in get_role_permission(kwargs.get("request")).get(Service.MarketerAdmin.name)
                        p
                        in kwargs.get("role_perm").get(
                            "roles"
                        )  # Service.MarketerAdmin.name)
                        for p in permissions
                    ]
                ):
                    logger.error("Unauthorized User")
                    logger.exception("Unauthorized User")
                    raise HTTPException(status_code=401, detail="Unauthorized User")
            return await func(*args, **kwargs)

        return wrapper

    return decorator
