from fastapi import APIRouter, Depends, Request, HTTPException
from src.tools.tokens import JWTBearer, get_sub
from src.tools.database import get_database
from src.schemas.marketer import MarketerOut, ModifyMarketerIn
from fastapi_pagination import Page, add_pagination
from fastapi_pagination.ext.pymongo import paginate


profile = APIRouter(prefix='/profile')


@profile.get("/marketers", dependencies=[Depends(JWTBearer())], tags=["Profile"], response_model=Page[MarketerOut])
async def get_marketer(request: Request):
    user_id = get_sub(request)

    if user_id != "4cb7ce6d-c1ae-41bf-af3c-453aabb3d156":
        raise HTTPException(status_code=403, detail="Not authorized.")
    
    database = get_database()

    marketer_coll = database["marketers"]


    return paginate(marketer_coll, {})


@profile.put("/modify-marketer", dependencies=[Depends(JWTBearer())], tags=["Profile"])
async def modify_marketer(request: Request, args: ModifyMarketerIn = Depends(ModifyMarketerIn)):

    user_id = get_sub(request)

    if user_id != "4cb7ce6d-c1ae-41bf-af3c-453aabb3d156":
        raise HTTPException(status_code=403, detail="Not authorized.")

    
    database = get_database()

    marketer_coll = database["marketers"]

    filter = {"IdpId": args.IdpId}
    update = {"$set": {}}


    if args.InvitationLink != None:
        update["$set"]["InvitationLink"] = args.InvitationLink

    if args.Mobile != None:
        update["$set"]["Mobile"] = args.Mobile

    modified_record = marketer_coll.update_one(filter, update)

    
    return modified_record.raw_result


add_pagination(profile)