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
from dds_repository import DdsRepository

class DdsProcessor:
    def __init__(self,
				 consumer: KafkaConsumer, 
				 producer: KafkaProducer, 
				 redis_client: RedisClient, 
				 pg_connect: PgConnect,
				 dds_repository: DdsRepository,
				 batch_size: int,
				 logger: Logger,
				 ) -> None:
        self._consumer = consumer
        self._producer = producer
        self._redis = redis_client
		self._pg_connect = PgConnect
        self._dds_repository = dds_repository        
        self._logger = logger
        self._batch_size = batch_size

    # функция, которая будет вызываться по расписанию.
    def run(self) -> None:
        # Пишем в лог, что джоб был запущен.
        self._logger.info(f"{datetime.utcnow()}: START")	
		
		# сначала заберем в словарь весь редис что есть (и подписчики и рестораны там лежат в куче)
		RedisClientSTG = self._dds_repository.RedisClient(AppConfig.redis_host, 
															AppConfig.redis_port, 
															AppConfig.redis_password, 
															AppConfig.CERTIFICATE_PATH)
		redis_data = RedisClientSTG.get()		
		# отправим в топик stg-service-orders
		R_toKafka = self._dds_repository.KafkaProducer(AppConfig.kafka_host, 
																AppConfig.kafka_port, 
																AppConfig.kafka_producer_user,
																AppConfig.kafka_producer_password,
																'stg-service-orders',
																AppConfig.CERTIFICATE_PATH)
		R_toKafka.produce(redis_data);			
		#забираем данные из stg слоя в postgre
		PG_fromSTG = self._dds_repository.PgConnect(AppConfig.pg_warehouse_host,
															AppConfig.pg_warehouse_port,
															AppConfig.pg_warehouse_dbname,
															AppConfig.pg_warehouse_user,
															AppConfig.pg_warehouse_password)
		postgre_data = PG_fromSTG.get()				
		#и направляем в топик dds-service-orders
		PGSTG_toKafka = self._dds_repository.KafkaProducer(AppConfig.kafka_host, 
																AppConfig.kafka_port, 
																AppConfig.kafka_producer_user,
																AppConfig.kafka_producer_password,
																'dds-service-orders',
																AppConfig.CERTIFICATE_PATH)
		PGSTG_toKafka.produce(postgre_data)
		# заберем данные из топика stg-service-orders
		R_fromKafka = self._dds_repository.KafkaConsumer(AppConfig.kafka_host, 
																AppConfig.kafka_port, 
																AppConfig.kafka_producer_user,
																AppConfig.kafka_producer_password,
																'stg-service-orders',
																'test_group',
																AppConfig.CERTIFICATE_PATH)
		redis_data = R_fromKafka.consume()				
		# заберем данные из топика dds-service-orders
		PG_fromKafka = self._dds_repository.KafkaConsumer(AppConfig.kafka_host, 
																AppConfig.kafka_port, 
																AppConfig.kafka_producer_user,
																AppConfig.kafka_producer_password,
																'dds-service-orders',
																'test_group',
																AppConfig.CERTIFICATE_PATH)
		postgre_data = PG_fromKafka.consume()			

		# и запишем их в dds слой
		R_toDDS = self._dds_repository.PgConnect(AppConfig.pg_warehouse_host,
													AppConfig.pg_warehouse_port,
													AppConfig.pg_warehouse_dbname,
													AppConfig.pg_warehouse_user,
													AppConfig.pg_warehouse_password,
													redis_data)

		for rows in R_toDDS._data():
			#если запись из редиса - про меню и в нем есть категория
			menu = rows['menu'] 
			if menu:
				R_toDDS.h_restaurant_insert(rows['_id'])
				R_toDDS.s_restaurant_names_insert(rows['_id'],rows['name'])
				R_toDDS.h_product_insert(menu['_id'])
				R_toDDS.s_product_names_insert(menu['_id'],menu['name'])
				R_toDDS.l_product_restaurant_insert(menu['_id'],rows['_id'])
				#схема данных недоработана, т.к. в redis нет поля id категории с uuid, это недоработка курса. предположим что оно есть :)
				R_toDDS.l_product_category_insert(menu['_id'],menu['category_id'])
				#чтобы не дублировалось
				if not R_toDDS.h_category_select(menu['category']):
					R_toDDS.h_category_insert(menu['category'])
			else:
				R_toDDS.s_user_names_insert(rows['_id'], rows['name'], rows['login'])
				R_toDDS.h_user_insert(rows['_id'])
		
		#загружаем что получили из postgre
		#здесь данные одинаковые, не как в редисе, и косяка с category_id нет,
		#поэтому проверять на дубликаты и пр не нужно
		PG_toDDS = self._dds_repository.PgConnect(AppConfig.pg_warehouse_host,
															AppConfig.pg_warehouse_port,
															AppConfig.pg_warehouse_dbname,
															AppConfig.pg_warehouse_user,
															AppConfig.pg_warehouse_password,
															postgre_data)
		for rows in PG_toDDS._data():
			PG_toDDS.h_order_insert(rows['object_id'])
			PG_toDDS.s_order_status_insert(rows['object_id'],rows['payload']['statuses']['final_staus'])
			PG_toDDS.s_order_cost(rows['object_id'],rows['payload']['cost'],rows['payload']['payment'])
			PG_toDDS.l_order_user_insert(rows['object_id'],rows['payload']['user'])
			PG_toDDS.l_order_product_insert(rows['object_id'],rows['payload']['order_items']['id'])

        # Пишем в лог, что джоб успешно завершен.
        self._logger.info(f"{datetime.utcnow()}: FINISH")


