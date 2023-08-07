from jwt.exceptions import InvalidIssuerError, ExpiredSignatureError
import jwt
from fastapi import Request, HTTPException
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from src.config import settings
from src.tools.jwksutils import rsa_pem_from_jwk
import requests
from requests.exceptions import ConnectionError
from json import loads
from src.tools.logger import logger

# obtain jwks as you wish: configuration file, HTTP GET request to the endpoint returning them;

try:
    jwks_req = requests.get(settings.JWKS_CONFIGURATION_URL)
    if jwks_req.status_code == 200:
        jwks = loads(jwks_req.content)

except ConnectionError as err:
    logger.error("Cannot connect to get IDP configurations...")
except Exception as err:
    logger.error(f"Error in getting IDP Configurations: {err}")
else:
    logger.info("Successfully got the IDP configurations...")


# configuration, these can be seen in valid JWTs from Azure B2C:
valid_audiences = settings.APPLICATION_ID
issuer = settings.ISSUER


class InvalidAuthorizationToken(Exception):
    """_summary_

    Args:
        Exception (_type_): _description_
    """

    def __init__(self, details):
        """_summary_

        Args:
            details (_type_): _description_
        """
        super().__init__("Invalid authorization token: " + details)


def get_kid(token):
    """_summary_

    Args:
        token (_type_): _description_

    Raises:
        InvalidAuthorizationToken: _description_
        InvalidAuthorizationToken: _description_

    Returns:
        _type_: _description_
    """
    headers = jwt.get_unverified_header(token)
    if not headers:
        raise InvalidAuthorizationToken("missing headers")
    try:
        return headers["kid"]
    except KeyError:
        raise InvalidAuthorizationToken("missing kid")


def get_jwk(kid):
    """_summary_

    Args:
        kid (_type_): _description_

    Raises:
        InvalidAuthorizationToken: _description_

    Returns:
        _type_: _description_
    """
    for jwk in jwks.get("keys"):
        if jwk.get("kid") == kid:
            return jwk
    raise InvalidAuthorizationToken("kid not recognized")


def get_public_key(token):
    """_summary_

    Args:
        token (_type_): _description_

    Returns:
        _type_: _description_
    """
    return rsa_pem_from_jwk(get_jwk(get_kid(token)))


def validate_jwt(jwt_to_validate):
    """_summary_

    Args:
        jwt_to_validate (_type_): _description_

    Returns:
        _type_: _description_
    """
    public_key = get_public_key(jwt_to_validate)

    try:
        decoded = jwt.decode(
            jwt_to_validate,
            public_key,
            verify=True,
            algorithms=["RS256"],
            #  audience=valid_audiences,
            issuer=issuer,
        )

        # do what you wish with decoded token:
        # if we get here, the JWT is validated
        return True
    except InvalidIssuerError as err:
        logger.error(f"Invalid issuer: \n {err}")
    except ExpiredSignatureError as err:
        logger.exception(f"Signature has expired: \n {err}")
    except Exception as error:
        logger.error(f"Wrong JWT {error}")
    return False


class JWTBearer(HTTPBearer):
    """_summary_

    Args:
        HTTPBearer (_type_): _description_
    """

    def __init__(self, auto_error: bool = True):
        """_summary_

        Args:
            auto_error (bool, optional): _description_. Defaults to True.
        """
        super(JWTBearer, self).__init__(auto_error=auto_error)

    async def __call__(self, request: Request):
        """_summary_

        Args:
            request (Request): _description_

        Raises:
            HTTPException: _description_
            HTTPException: _description_
            HTTPException: _description_

        Returns:
            _type_: _description_
        """
        credentials: HTTPAuthorizationCredentials = await super(
            JWTBearer, self
        ).__call__(request)
        if credentials:
            if not credentials.scheme == "Bearer":
                raise HTTPException(
                    status_code=401, detail="Invalid authentication scheme."
                )
            if not self.verify_jwt(credentials.credentials):
                raise HTTPException(
                    status_code=401, detail="Invalid token or expired token."
                )
            return credentials.credentials
        else:
            raise HTTPException(status_code=401, detail="Invalid authorization code.")

    def verify_jwt(self, jwtoken: str) -> bool:
        """_summary_

        Args:
            jwtoken (str): _description_

        Returns:
            bool: _description_
        """
        isTokenValid: bool = False

        try:
            payload = validate_jwt(jwtoken)
        except:
            payload = None
        if payload:
            isTokenValid = True
        return isTokenValid


def get_role_permission(req: Request):
    """_summary_

    Args:
        req (Request): _description_

    Returns:
        _type_: _description_
    """
    token = req.headers.get("authorization").split()[1]
    public_key = get_public_key(token)

    decoded = jwt.decode(
        token, public_key, verify=True, algorithms=["RS256"], issuer=issuer
    )
    logger.info(decoded)
    role_perm = {}
    role_perm["sub"] = decoded["sub"]
    role_perm["client_id"] = decoded["client_id"]
    try:
        role_perm["roles"] = decoded[
            "http://schemas.microsoft.com/ws/2008/06/identity/claims/role"
        ]
    except:
        role_perm["roles"] = []
    role_perm["scopes"] = decoded["scope"]
    # if decoded['CustomerManagement']:
    #     role_perm['CustomerManagement'] = decoded['CustomerManagement']
    # else:
    #     role_perm['CustomerManagement'] = ''
    try:
        role_perm["Marketer"] = decoded["Marketer"]
    except:
        role_perm["Marketer"] = []
    try:
        role_perm["roles"] = decoded["MarketerAdmin"]
    except:
        role_perm["roles"] = []

    return role_perm  # e892eae7-cf2e-48d8-a024-a7c9eb0f8668
    # return "4cb7ce6d-c1ae-41bf-af3c-453aabb3d156"
