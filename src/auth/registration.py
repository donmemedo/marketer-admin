import json

import aiohttp
from fastapi import HTTPException
from src.config import settings


async def get_token():
    headers = {"Content-Type": "application/x-www-form-urlencoded"}

    payload = {
        "client_id": settings.CLIENT_ID,
        "client_secret": settings.CLIENT_SECRET,
        "grant_type": settings.GRANT_TYPE,
    }

    try:
        async with aiohttp.ClientSession(
            connector=aiohttp.TCPConnector(verify_ssl=False)
        ) as session:
            async with session.post(
                url=settings.TOKEN_URL, headers=headers, data=payload
            ) as response:
                response.raise_for_status()
                res = await response.json()

                return res.get("access_token")
    except aiohttp.ClientError as err:
        raise HTTPException(status_code=500, detail="Failed to get token from IDP")


async def set_permissions(permissions, token):
    headers = {
        "accept": "*/*",
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }

    payload = json.dumps({"registerServicePermissionItems": permissions})

    try:
        async with aiohttp.ClientSession(
            connector=aiohttp.TCPConnector(verify_ssl=False)
        ) as session:
            async with session.post(
                url=settings.REGISTRATION_URL, headers=headers, data=payload
            ) as response:
                response.raise_for_status()
                return await response.json()

    except aiohttp.ClientError as err:
        raise HTTPException(
            status_code=500, detail="Failed to register permissions in IDP server"
        )
