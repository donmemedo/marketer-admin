"""_summary_

Returns:
    _type_: _description_
"""
import grpc
from fastapi import APIRouter, Depends, Request
from google.protobuf.json_format import MessageToDict
from pymongo import MongoClient
from validator import Validator

from src.auth.authentication import get_role_permission
from src.auth.authorization import authorize
# from src.tools.database import get_db
# from src.models.model import BankCreditLines
from src.protos import marketer_pb2, marketer_pb2_grpc  # ,customTypes_pb2,customTypes_pb2_grpc
from src.schemas.marketer import *
from src.tools.database import get_database
from src.tools.logger import logger

# from sqlalchemy import or_, and_
# from sqlalchemy.exc import IntegrityError, OperationalError
# from src.tools.errors import database_error, create_error_response


marketer = APIRouter(prefix="/marketer")
marketer_relation = APIRouter(prefix="/marketer-relation")
grpc_services = APIRouter(prefix="/grpc")
channel = grpc.insecure_channel('172.24.65.20:9035')
# channel = grpc.secure_channel('172.24.65.20:9035',grpc.)
@grpc_services.get(
    "/marketer-sync",
    tags=["GRPC"],
)
@authorize(
    [
        "MarketerAdmin.All.Create",
        "MarketerAdmin.All.All",
        "MarketerAdmin.Marketer.Read",
        "MarketerAdmin.Marketer.Update",
        "MarketerAdmin.Marketer.All",
    ]
)
async def sync_marketers(
    request: Request,
    args: Pages = Depends(Pages),
    database: MongoClient = Depends(get_database),
    role_perm: dict = Depends(get_role_permission),
):
    """_summary_

    Args:
        request (Request): _description_
        args (AddMarketerIn, optional): _description_. Defaults to Depends(AddMarketerIn).

    Returns:
        _type_: _description_
    """
    stub = marketer_pb2_grpc.MarketerRPCServiceStub(channel)

    sync_request = marketer_pb2.GetAllMarketersRPCRequest()

    response = stub.GetAllMarketers(sync_request)

    print("Response:", response)




class Syncer(marketer_pb2_grpc.MarketerRPCService):
    def SyncMarketers(self, request, context):
        validate_request = MessageToDict(request, preserving_proto_field_name=True)
        rules = {
            "size": "min:1",
            "page": "min:0"
        }
        val = Validator(validate_request, rules)

        if val.validate():
            print("Data is valid")
        else:
            print("Validation errors:", val.errors)
        update = {"$set": {}}

        update["$set"]["Id"]=request.Id,
        update["$set"]["UserId"]=request.UserId,
        update["$set"]["CustomerId"]=request.CustomerId,
        update["$set"]["UniqueId"]=request.UniqueId,
        update["$set"]["Title"]=request.Title,
        update["$set"]["Type"]=request.Type,
        update["$set"]["TypeTitle"]=request.TypeTitle,
        update["$set"]["Mobile"]=request.Mobile,
        update["$set"]["SubsidiaryId"]=request.SubsidiaryId,
        update["$set"]["SubsidiaryTitle"]=request.SubsidiaryTitle,
        update["$set"]["BranchId"]=request.BranchId,
        update["$set"]["BranchTitle"]=request.BranchTitle,
        update["$set"]["MarketerRefCode"]=request.MarketerRefCode,
        update["$set"]["MarketerRefLink"]=request.MarketerRefLink,
        update["$set"]["ReagentRefCode"]=request.ReagentRefCode,
        update["$set"]["ReagentRefLink"]=request.ReagentRefLink,
        update["$set"]["TbsMarketerId"]=request.TbsMarketerId,
        update["$set"]["TbsMarketerName"]=request.TbsMarketerName,
        update["$set"]["TbsReagentId"]=request.TbsReagentId,
        update["$set"]["TbsReagentName"]=request.TbsReagentName,
        update["$set"]["IsActive"]=request.IsActive,
        update["$set"]["CreateDateTime"]=request.CreateDateTime,
        update["$set"]["UpdateDateTime"]=request.UpdateDateTime,

        db = get_database()
        try:
            db.newmarketersss.insert_one(update["$set"])
            result = {
                "MarketerID": str(update["$set"]["Id"])
            }
            logger.info("Marketer is inserted successfully")
            return marketer_pb2.ListGetMarketersRPCResponse(
                IsFailed=False,
                Error="",
                Result=update["$set"]
            )
        except:
            try:
                db.newmarketersss.update_one({"Id":update["$set"]["id"]},update)
                result = {
                    "MarketerID": str(update["$set"]["Id"])
                }
                logger.info("Marketer is updated successfully")
                return marketer_pb2.ListGetMarketersRPCResponse(
                    IsFailed=False,
                    Error="",
                    Result=update["$set"]
                )
            except:
                logger.info("Insertion or Update is unsuccessful.")
                return marketer_pb2.MarketerRPCError(
                    ErrorMessage="Can't Insert or Update."
                )
