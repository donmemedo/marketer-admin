"""_summary_

Returns:
    _type_: _description_
"""
import grpc
from datetime import datetime
from grpc._channel import _InactiveRpcError
from fastapi.responses import JSONResponse
from fastapi import APIRouter, Depends, Request
from google.protobuf.json_format import MessageToDict
from pymongo import MongoClient
from validator import Validator
from src.config import settings
from src.auth.authentication import get_role_permission
from src.auth.authorization import authorize
from src.protos import (
    marketer_pb2,
    marketer_pb2_grpc,
    customTypes_pb2,
    customTypes_pb2_grpc,
)
from src.schemas.marketer import *
from src.tools.database import get_database
from src.tools.logger import logger


grpc_services = APIRouter(prefix="/grpc")
channel = grpc.insecure_channel(f"{settings.GRPC_IP}:{settings.GRPC_PORT}")


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
    metadata = []
    auth_token = request.headers.get("authorization")
    metadata.append(("authorization", auth_token))
    sync_request = marketer_pb2.SearchMarketerRPCRequest(
        # ToDo: When FrontEnd giving these args then change
        # PageSize=args.size, PageNumber=args.page - 1
        PageSize=10000,
        PageNumber=0,
    )

    try:
        response = stub.SearchMarketer(sync_request, metadata=metadata)
    except _InactiveRpcError as err:
        logger.error(err)
        return err.debug_error_string()
    validate_request = MessageToDict(response, preserving_proto_field_name=True)
    rules = {"size": "min:1", "page": "min:0"}
    val = Validator(validate_request, rules)

    if val.validate():
        print("Data is valid")
    else:
        print("Validation errors:", val.errors)
    # db = get_database()
    marketer_coll = database[settings.MARKETER_COLLECTION]
    ins_results = []
    up_results = []
    for marketer in validate_request["PagedData"]:
        update = {"$set": {}}
        for key, value in marketer.items():
            if value is not None:
                if type(value) is dict:
                    update["$set"][key] = value["value"]
                else:
                    update["$set"][key] = value
        try:
            update["$set"]["MarketerID"] = update["$set"].pop("UserId")
            update["$set"].pop("Id")

        except:
            update["$set"]["MarketerID"] = update["$set"].pop("Id")

        try:
            # update["$set"]["MarketerID"] = update["$set"].pop("Id")
            marketer_coll.insert_one(update["$set"])
            try:
                result = f"Marketer {marketer['TbsReagentName']} with ID {marketer['Id']['value']} is inserted successfully."
                logger.info(result)
            except:
                try:
                    result = f"Marketer {marketer['Title']} with ID {marketer['Id']['value']} is inserted successfully."
                    logger.info(result)
                except:
                    result = f"Marketer with ID {marketer['Id']['value']} is inserted successfully."
                    logger.info(result)

            ins_results.append(marketer)
        except:
            try:
                update["$set"].pop("_id")
                # ToDo: Check if something changes then update.
                # marketer_coll.update_one({"MarketerID": update["$set"]["MarketerID"]}, update)
                marketer_coll.update_one(
                    {"MarketerID": update["$set"]["MarketerID"]}, update
                )
                try:
                    result = f"Marketer {marketer['TbsReagentName']} with ID {marketer['Id']['value']} is updated successfully."
                    logger.info(result)
                except:
                    try:
                        result = f"Marketer {marketer['Title']} with ID {marketer['Id']['value']} is updated successfully."
                        logger.info(result)
                    except:
                        result = f"Marketer with ID {marketer['Id']['value']} is updated successfully."
                        logger.info(result)

                up_results.append(marketer)
            except:
                response = f"Insertion or Update is unsuccessful because of Database Connection Failure in {datetime.now().isoformat()}"
                logger.critical(response)
                return JSONResponse(status_code=500, content=response)
    response = {
        "Inserted Marketers": ins_results,
        "Updated Marketers": up_results,
        "timeGenerated": datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
        "error": "",
    }
    return JSONResponse(status_code=200, content=response)
