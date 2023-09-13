"""_summary_

Returns:
    _type_: _description_
"""
from datetime import date

from khayyam import JalaliDatetime


def remove_id(items: list):
    """_summary_

    Args:
        items (list): _description_

    Returns:
        _type_: _description_
    """
    for item in items:
        if "_id" in item:
            del item["_id"]

    return items


def to_gregorian(date_: date):
    """_summary_

    Args:
        date_ (date): _description_

    Returns:
        _type_: _description_
    """
    jalali_date = JalaliDatetime(year=date_.year, month=date_.month, day=date_.day)

    gregorian_date = jalali_date.todate().strftime("%Y-%m-%d")
    return gregorian_date


def to_gregorian_(date_string: str):
    """_summary_

    Args:
        date_string (str): _description_

    Returns:
        _type_: _description_
    """
    year, month, day = date_string.split("-")

    return JalaliDatetime(year, month, day).todate().strftime("%Y-%m-%d")


def peek(iterable):
    """_summary_

    Args:
        iterable (_type_): _description_

    Returns:
        _type_: _description_
    """
    try:
        first = next(iterable)
    except StopIteration:
        return None
    return first


def get_marketer_name(marketer_dict: dict):
    """_summary_

    Args:
        marketer_dict (dict): _description_

    Returns:
        _type_: _description_
    """
    if marketer_dict is None:
        return "There is No Marketer with is IdpID."
    if marketer_dict.get("FirstName") == "":
        return marketer_dict.get("LastName")
    if marketer_dict.get("LastName") == "":
        return marketer_dict.get("FirstName")
    return marketer_dict.get("FirstName") + " " + marketer_dict.get("LastName")


def marketer_entity(marketer) -> dict:
    """_summary_

    Args:
        marketer (_type_): _description_

    Returns:
        dict: _description_
    """
    return {
        "MarketerID": marketer.get("MarketerID"),
        "FirstName": marketer.get("FirstName"),
        "LastName": marketer.get("LastName"),
        "IsOrganization": marketer.get("IsOrganization"),
        "RefererType": marketer.get("RefererType"),
        "CreatedBy": marketer.get("CreatedBy"),
        "CreateDate": marketer.get("CreateDate"),
        "ModifiedBy": marketer.get("ModifiedBy"),
        "ModifiedDate": marketer.get("ModifiedDate"),
        "IsCustomer": marketer.get("IsCustomer"),
        "IsEmployee": marketer.get("IsEmployee"),
        "CustomerType": marketer.get("CustomerType"),
        "MarketerID": marketer.get("MarketerID"),
        "InvitationLink": marketer.get("InvitationLink"),
    }


def check_permissions(permissions, allow) -> bool:
    if any(x in permissions for x in allow):
        return True
    else:
        return False
