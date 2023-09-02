import logging
import socket
from logging.config import dictConfig

from src.config import settings

log_config = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "access": {
            "()": "uvicorn.logging.AccessFormatter",
            "fmt": '%(levelprefix)s %(asctime)s - %(client_addr)s - "%(request_line)s" %(status_code)s',
            "datefmt": "%Y-%m-%d %H:%M:%S",
            "use_colors": True,
        },
        "default": {
            "()": "uvicorn.logging.DefaultFormatter",
            "fmt": "%(levelprefix)s %(asctime)s - %(message)s",
            "datefmt": "%Y-%m-%d %H:%M:%S",
            "use_colors": True,
        },
        "json": {
            "()": "pythonjsonlogger.jsonlogger.JsonFormatter",
            "format": "%(asctime)s %(created)f %(exc_info)s %(filename)s %(funcName)s %(levelname)s %(modulename)s %(levelno)s %(lineno)d %(module)s %(message)s %(pathname)s %(process)s %(processName)s %(relativeCreated)d %(thread)s %(threadName)s",
        },
        "simple": {"format": "%(asctime)s %(levelname)s %(message)s"},
    },
    "handlers": {
        "access": {
            "class": "logging.StreamHandler",
            "formatter": "access",
            "stream": "ext://sys.stdout",
        },
        "default": {
            "formatter": "default",
            "class": "logging.StreamHandler",
            "stream": "ext://sys.stderr",
        },
        "splunk": {
            "class": "logging.handlers.SysLogHandler",
            "address": (settings.SPLUNK_HOST, settings.SPLUNK_PORT),
            "socktype": socket.SOCK_DGRAM,
            "formatter": "json",
        },
    },
    "loggers": {
        "marketer-admin": {
            "handlers": ["splunk", "default"],
            "level": "DEBUG",
            "propagate": True,
        },
        "uvicorn": {
            "handlers": ["default", "splunk"],
            "level": "DEBUG",
            "propagate": False,
        },
        "uvicorn.access": {
            "handlers": ["access", "splunk"],
            "level": "INFO",
            "propagate": False,
        },
        "uvicorn.error": {"level": "INFO", "propagate": False},
    },
}


dictConfig(log_config)
logger = logging.getLogger("marketer-admin")
