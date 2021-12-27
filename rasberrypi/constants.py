import logging
import sys
import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(BASE_DIR)

auto_upload_configuration = {"debug": 1,
                             "log_file": 'auto_upload.log',
                             "mode": 1,
                             "enable_redis": True,
                             "redis_db": 1,
                             "log_level": logging.DEBUG,
                             "config_file_path": os.path.join(BASE_DIR, "config.json"),
                             "server": ''}

