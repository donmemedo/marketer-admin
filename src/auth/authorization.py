from functools import wraps

from fastapi import HTTPException

from src.tools.logger import logger


def authorize(permissions):
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            if kwargs.get("role_perm").get("roles"):
                if not any(
                    [p in kwargs.get("role_perm").get("roles") for p in permissions]
                ):
                    logger.error("Unauthorized User")
                    logger.exception("Unauthorized User")
                    raise HTTPException(status_code=403, detail="Unauthorized User")
            return await func(*args, **kwargs)

        return wrapper

    return decorator
