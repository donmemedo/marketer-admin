import aiohttp
from fastapi import Depends, HTTPException
from fastapi.security import HTTPBearer
from fastapi.security.http import HTTPAuthorizationCredentials
from jose import jwt
from jose.exceptions import ExpiredSignatureError
from src.config import settings
from src.tools.logger import logger

bearer_scheme = HTTPBearer()


async def get_public_key() -> str:
    try:
        async with aiohttp.ClientSession(
            connector=aiohttp.TCPConnector(verify_ssl=False)
        ) as session:
            async with session.get(settings.OPENID_CONFIGURATION_URL) as response:
                response.raise_for_status()
                config = await response.json()
                jwks_uri = config["jwks_uri"]
                async with session.get(jwks_uri) as jwks_response:
                    jwks_response.raise_for_status()
                    jwks = await jwks_response.json()
                    public_key = jwks["keys"][0]["x5c"][0]
                    return public_key
    except (aiohttp.ClientError, KeyError, IndexError) as err:
        logger.exception(f"Cannot connect to IDP: {err}")
        logger.error(f"Cannot connect to IDP: {err}")
        raise HTTPException(status_code=500, detail="Failed to fetch public key")


def verify_token(token: str, public_key: str) -> dict:
    try:
        payload = jwt.decode(
            token,
            public_key,
            algorithms=["RS256"],
            options={"verify_signature": False, "verify_aud": False},
        )
        return payload
    except jwt.JWTError as err:
        logger.error(f"Invalid token: {err}")
        logger.exception(f"Invalid token: {err}")
        raise HTTPException(status_code=401, detail="Invalid token")


async def get_current_user(
    token: HTTPAuthorizationCredentials = Depends(bearer_scheme),
):
    if token.scheme != "Bearer":
        logger.exception("Invalid authentication scheme")
        logger.error("Invalid authentication scheme")
        raise HTTPException(status_code=401, detail="Invalid authentication scheme")

    public_key = await get_public_key()
    return verify_token(token.credentials, public_key)


def get_role_permission(
    token: HTTPAuthorizationCredentials = Depends(bearer_scheme),
):
    """_summary_

    Args:
        req (Request): _description_

    Returns:
        _type_: _description_
    """
    public_key = get_public_key()

    try:
        decoded = jwt.decode(
            token.credentials,
            public_key,
            algorithms=["RS256"],
            options={"verify_signature": False, "verify_aud": False},
        )
    except ExpiredSignatureError as err:
        logger.error(msg=err)
        raise HTTPException(status_code=401, detail=str(err))

    logger.info(decoded)
    role_perm = {}
    role_perm["sub"] = decoded["sub"]
    role_perm["client_id"] = decoded["client_id"]
    try:
        role_perm["roles"] = decoded["permission"]
    except:
        role_perm["roles"] = []
    role_perm["scopes"] = decoded["role"]
    try:
        role_perm["Marketer"] = decoded["Marketer"]
    except:
        role_perm["Marketer"] = []
    return role_perm
