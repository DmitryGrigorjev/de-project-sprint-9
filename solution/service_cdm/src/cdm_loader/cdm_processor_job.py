import time
import json
import psycopg
import logging

from datetime import datetime
from typing import Dict, Optional,Generator
from contextlib import contextmanager
from psycopg import Connection
from app_config import AppConfig
from lib.pg import PgConnect
from cdm_repository import CdmRepository

class CdmProcessor:
    def __init__(self,
				 pg_connect: PgConnect,
				 cdm_repository: CdmRepository,
				 batch_size: int,
				 logger: Logger,
				 ) -> None:
		self._pg_connect = PgConnect(AppConfig.pg_warehouse_host,AppConfig.pg_warehouse_port,AppConfig.pg_warehouse_dbname,AppConfig.pg_warehouse_user,AppConfig.pg_warehouse_password)
        self._cdm_repository = cdm_repository        
        self._logger = logger
        self._batch_size = 100

    # функция, которая будет вызываться по расписанию.
    def run(self) -> None:
        # Пишем в лог, что джоб был запущен.
        self._logger.info(f"{datetime.utcnow()}: START")
		self._logger.info(f"{datetime.utcnow()}: Deleting UserCategories DM")        
        self._cdm_repository.UserCategoriesDelete(self._pg_connect) 
		self._logger.info(f"{datetime.utcnow()}: Inserting UserCategories DM")  
        self._cdm_repository.UserCategoriesWrite(self._pg_connect) 
		self._logger.info(f"{datetime.utcnow()}: Deleting UserProducts DM")  
        self._cdm_repository.UserProductsDelete(self._pg_connect) 
		self._logger.info(f"{datetime.utcnow()}: Deleting UserProducts DM")  
        self._cdm_repository.UserProductsWrite(self._pg_connect)
        # Пишем в лог, что джоб успешно завершен.
        self._logger.info(f"{datetime.utcnow()}: FINISH")


