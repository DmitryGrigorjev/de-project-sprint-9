import time
import json
import psycopg
import redis
import logging

from datetime import datetime
from typing import Dict, Optional,Generator
from contextlib import contextmanager
from psycopg import Connection
from app_config import AppConfig
from lib.pg import PgConnect
from lib.redis import RedisClient
from dds_repository import DdsRepository

class DdsProcessor:
    def __init__(self,
				 redis_client: RedisClient, 
				 pg_connect: PgConnect,
				 dds_repository: DdsRepository,
				 batch_size: int,
				 logger: Logger,
				 ) -> None:
        self._redis = RedisClient(AppConfig.redis_host, AppConfig.redis_port, AppConfig.redis_password, AppConfig.redis_certificate)
		self._pg_connect = PgConnect(AppConfig.pg_warehouse_host,AppConfig.pg_warehouse_port,AppConfig.pg_warehouse_dbname,AppConfig.pg_warehouse_user,AppConfig.pg_warehouse_password)
        self._dds_repository = dds_repository        
        self._logger = logger
        self._batch_size = 100

    # функция, которая будет вызываться по расписанию.
    def run(self) -> None:
        # Пишем в лог, что джоб был запущен.
        self._logger.info(f"{datetime.utcnow()}: START")
		self._logger.info(f"{datetime.utcnow()}: Writing from REDIS")
        self._dds_repository.WriteFromRedis(self._pg_connect,self._redis)
		self._logger.info(f"{datetime.utcnow()}: Writing from POSTGRE")        
        self._dds_repository.WriteFromPostgre(self._pg_connect,self._pg_connect)
        # Пишем в лог, что джоб успешно завершен.
        self._logger.info(f"{datetime.utcnow()}: FINISH")


