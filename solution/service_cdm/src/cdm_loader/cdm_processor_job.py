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

		#заберем данные из топика cdm-service-orders
		CATS_fromKafka = self._cdm_repository.KafkaConsumer(AppConfig.kafka_host, 
																AppConfig.kafka_port, 
																AppConfig.kafka_producer_user,
																AppConfig.kafka_producer_password,
																'cdm-service-orders',
																'test_group',
																AppConfig.CERTIFICATE_PATH)
		cats_data = CATS_fromKafka.consume()			
		#очистим первую витрину
        self._cdm_repository.UserCategoriesDelete(self._pg_connect) 			
		#и запишем ее по новой
		for rows in cats_data._data():
			cats_data.UserCategoriesWrite(rows['user_id'],rows['category_id'],rows['category_name'],rows['order_cnt'])

		#заберем данные из топика cdm-service-orders
		PRODS_fromKafka = self._cdm_repository.KafkaConsumer(AppConfig.kafka_host, 
																AppConfig.kafka_port, 
																AppConfig.kafka_producer_user,
																AppConfig.kafka_producer_password,
																'cdm-service-orders',
																'test_group',
																AppConfig.CERTIFICATE_PATH)
		prods_data = PRODS_fromKafka.consume()			 
		#очистим вторую витрину
        self._cdm_repository.UserProductsDelete(self._pg_connect) 		
		#и запишем первую витрину
		for rows in prods_data._data():
			prods_data.UserProductsWrite(rows['user_id'],rows['product_id'],rows['product_name'],rows['order_cnt'])
			
        # Пишем в лог, что джоб успешно завершен.
        self._logger.info(f"{datetime.utcnow()}: FINISH")


