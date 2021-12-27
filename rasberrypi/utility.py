import sys
import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(BASE_DIR)
from datetime import datetime
import json
import logging
import time
import redis
from key_generators import make_key
from Logger import Logger


class RedisMgmt(object):
    def __init__(self, host='localhost', port=6379, db=0, password='', charset='utf-8', decode_responses=True, config_file_path='config.json'):
        print("*************************", config_file_path)
        config_json = json.loads(open(config_file_path).read())
        self.db_number = db
        self.r = redis.StrictRedis(host=host,
                                   port=port,
                                   db=db,
                                   charset=charset,
                                   decode_responses=decode_responses)

    def add_keys(self, vals, dev_id=''):
        """
        add a list of keys to user made list of keys
        :param vals: list of keys to be added (str)
        :param dev_id: device id (str) (default: str)
        :return: length of list after adding
        """
        if dev_id != '':
            return self.r.rpush(dev_id + '.keys', *vals)
        return self.r.rpush('keys', *vals)

    def rem_key(self, val, dev_id=''):
        """
        remove a key from the user made list of keys
        :param val: key to be deleted (str)
        :param dev_id: device id (str) (default: str)
        :return: number of removed keys
        """
        if dev_id != '':
            return self.r.lrem(dev_id + '.keys', 0, val)
        return self.r.lrem('keys', 0, val)

    def get_all_keys(self, dev_id=''):
        """
        gets all keys from user made list of keys
        :param dev_id: device id (str) (default: str)
        :return: all keys (list)
        """
        if dev_id != '':
            return self.r.lrange(dev_id + '.keys', 0, -1)
        return self.r.lrange('keys', 0, -1)

    def get_val(self, key, dev_id=''):
        """
        get key value for dev_id from redis
        :param key: like lat, lon, etc. (str)
        :param dev_id: device id (str) (default: str)
        :return: value of key (str)
        """
        if dev_id != '':
            return self.r.get(dev_id + '.' + key)
        return self.r.get(key)

    def set_val(self, key, val, dev_id=''):
        """
        set val of key of dev_id in redis
        :param key: key (str)
        :param val: value (int/float/str)
        :param dev_id: device id (str) (default: str)
        :return: True/False - success/failure (bool)
        """
        # keys = self.get_all_keys(dev_id=dev_id)
        # if key not in keys:
        #     self.add_keys([key], dev_id=dev_id)
        if dev_id != '':
            return self.r.set(dev_id + '.' + key, val)
        return self.r.set(key, val)

    def check_val(self, key, dev_id=''):
        """
        checks if key has a value stored in redis
        :param key: key (str)
        :param dev_id: device id (str) (default: '')
        :return: True if exists, otherwise False
        """
        val = self.get_val(key, dev_id)
        if val:
            return True
        return False

    def del_key(self, keys, dev_id=''):
        """
        deletes key from redis
        :param
        :return: number of keys deleted
        """
        if dev_id != '':
            keys = [dev_id + '.' + key for key in keys]
        return self.r.delete(*keys)

    def set_json(self, key, json):
        """
        adds json to redis
        :param key: key
        :param json: dict
        :return: True/False - success/failure (bool)
        """
        self.r.hmset(key, json)

    def get_json(self, key):
        """
        gets json from redis
        :param key: key (str)
        :return: dict
        """
        self.r.hgetall(key)

    @staticmethod
    def cache_it(key_prefix=None, timeout=None):
        """
        :param timeout: in seconds
        :param key_prefix:
        :param redis_connection: redis connection
        :return: cached heavy computation result
        """

        def cache_it_decorator(func):
            cache_key_prefix = key_prefix or func.__name__

            def cache_it_wrapper(*args, **kwargs):
                ut = kwargs.pop('utility')
                try:
                    redis_connection = ut.master_redis
                except AttributeError:
                    redis_connection = None

                if redis_connection is None:
                    redis_connection = RedisMgmt(db=12)
                delete_this = kwargs.pop('deleteThis', False)
                dummy_args = list(args)
                dummy_args.extend(kwargs.values())
                cache_key = "{}:{}".format(cache_key_prefix, make_key(*dummy_args))
                try:

                    if delete_this:
                        redis_connection.r.delete(cache_key)
                        return None

                    result = redis_connection.r.get(cache_key)

                    if not result:
                        result = func(*args, **kwargs)

                        redis_connection.r.set(cache_key, result, ex=timeout)
                except ConnectionError as err:
                    # logger.fatal(err)
                    if delete_this:
                        return None
                    return func(*args, **kwargs)
                return result

            return cache_it_wrapper

        return cache_it_decorator


class Utility(object):

    def __init__(self, configuration=None):
        if not configuration:
            self.configuration = {"debug": 0,
                                  "log_file": None,
                                  "mode": 1,
                                  "enable_redis": False,
                                  "redis_db": 0,
                                  "log_level": logging.DEBUG,
                                  "config_file_path": os.path.join(BASE_DIR, "config.json"),
                                  "server": ''}
        else:
            self.configuration = configuration
        self.logger = Logger(self.configuration.get("debug"), self.configuration.get("mode"), self.configuration.get("log_file"), self.configuration.get("log_level"), config_file_path=self.configuration.get("config_file_path"))
        self.enable_redis = self.configuration.get("enable_redis")
        self.redis_db = self.configuration.get("redis_db")
        self.config_file_path = self.configuration.get("config_file_path")
        self.debug = self.configuration.get("debug")
        self.mode = self.configuration.get("mode")
        self.server = self.configuration.get("server")
        self.tag = ""
        try:
            self.config_json = json.loads(open(self.config_file_path).read())
            self.config_json = self.config_json.get(self.config_json.get("env"))

        except Exception as e:
            self.loginfo("error reading config file: " + str(e), 'error')
        if self.enable_redis:
            self.master_redis = RedisMgmt(db=self.redis_db, config_file_path=self.config_file_path)

    def __setattr__(self, key, value):
        object.__setattr__(self, key, value)
        if key in ('debug', 'mode'):
            self.logger.__setattr__(key, value)

    def loginfo(self, message, level='info'):
        if self.tag != '':
            message = '{}: {}'.format(self.tag, message)
        self.logger.log(message, level)

