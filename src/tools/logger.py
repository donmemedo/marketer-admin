import logging
from logging.config import dictConfig
import socket
from src.config import settings


# class SplunkIndexFilter(logging.Filter):
#     def __init__(self, index) -> None:
#         super().__init__()
#         self.index = index
#
#     def filter(self, record):
#         record.index  = self.index
#         return True


log_config = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        'access': {
            '()': 'uvicorn.logging.AccessFormatter',
            'fmt': '%(levelprefix)s %(asctime)s - %(client_addr)s - "%(request_line)s" %(status_code)s',
            "datefmt": "%Y-%m-%d %H:%M:%S",
            "use_colors": True
        },
        "default": {
            "()": "uvicorn.logging.DefaultFormatter",
            "fmt": "%(levelprefix)s %(asctime)s - %(message)s",
            "datefmt": "%Y-%m-%d %H:%M:%S",
            "use_colors": True
        },
        'json': {
            '()': 'pythonjsonlogger.jsonlogger.JsonFormatter',
            'format': '%(asctime)s %(created)f %(exc_info)s %(filename)s %(funcName)s %(levelname)s %(levelno)s %(lineno)d %(module)s %(message)s %(pathname)s %(process)s %(processName)s %(relativeCreated)d %(thread)s %(threadName)s'
        },

        'simple': {
            'format': '%(asctime)s %(levelname)s %(message)s'

        },
    },
    # "filters": {
    #     "splunk_index": {
    #         # "()": "logging.Filter",
    #         "()":  SplunkIndexFilter,
    #         # "name": "splunk_index",
    #         "index": settings.SPLUNK_INDEX
    #     }
    # },
        "handlers": {
        'access': {
            'class': 'logging.StreamHandler',
            'formatter': 'access',
            'stream': 'ext://sys.stdout'
        },
        "default": {
            'formatter': 'default',
            'class': 'logging.StreamHandler',
            'stream': 'ext://sys.stderr',
        },
        "splunk": {
            'class': 'logging.handlers.SysLogHandler',
            'address': (settings.SPLUNK_HOST, settings.SPLUNK_PORT),
            'socktype': socket.SOCK_DGRAM,
            # "formatter": "simple"
            'formatter': 'json'
            #
            # # 'level': 'DEBUG',
            # # 'class': 'splunk_handler.SplunkHandler',
            # 'class': 'logging.handlers.DatagramHandler',
            #
            # # 'formatter': 'json',
            # 'host': settings.SPLUNK_HOST,
            # 'port': settings.SPLUNK_PORT,
            # 'formatter': 'simple',
            # 'filters': ["splunk_index"]

            # 'filters': [{
            #     'type': 'logging.Filter',
            #     'name': 'splunk_index',
            #     'index': settings.SPLUNK_INDEX
            # }]

            # 'timeout': 60,
            # 'debug': True,
            # 'token': '',
            # 'index': settings.SPLUNK_INDEX,
            # 'sourcetype': 'json',
            # 'source': ''
        }
    },
    "loggers": {
        "marketer-admin": {
            "handlers": ['default','splunk'],
            "level": "DEBUG",
            "propagate": False
        },
        "uvicorn": {
            "handlers": ['default','splunk'],
            "level": "DEBUG",
            "propagate": True
        },
        'uvicorn.access': {
            'handlers': ['access','splunk'],
            'level': 'INFO',
            'propagate': False
        },
        'uvicorn.error': {
            'level': 'INFO',
            'propagate': False
        }
    },
}


dictConfig(log_config)
logger = logging.getLogger("marketer-admin")
