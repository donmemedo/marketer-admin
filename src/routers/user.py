from fastapi import APIRouter, Depends, Request, HTTPException
from src.tools.tokens import JWTBearer, get_sub
from src.tools.database import get_database
from src.schemas.marketer import UserTradesIn, UserTradesOut
from fastapi_pagination import Page, add_pagination
from fastapi_pagination.ext.pymongo import paginate


user_router = APIRouter(prefix='/user')


@user_router.get("/user-trades", dependencies=[Depends(JWTBearer())], tags=["User"], response_model=Page[UserTradesOut])
async def get_marketer(request: Request, args: UserTradesIn = Depends(UserTradesIn)):
    user_id = get_sub(request)

    if user_id != "4cb7ce6d-c1ae-41bf-af3c-453aabb3d156":
        raise HTTPException(status_code=403, detail="Not authorized.")
    
    database = get_database()

    trades_coll = database["trades"]


    return paginate(trades_coll, {})


add_pagination(user_router)